package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/discovery"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"google.golang.org/grpc"
)

const (
	numIDDigits = 3
)

func main() {

	// Configure logger
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logger.Logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true})

	// Parse command line arguments.
	slaveTag := os.Args[1]
	masterAddr := os.Args[2]
	ownPublicIP := os.Args[3]
	ownPrivateIP := os.Args[4]

	// Set up a GRPC connection.
	conn, err := grpc.Dial(masterAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logger.Fatal().Str("masterAddr", masterAddr).Msg("Could not connect to master server.")
	}
	defer conn.Close()

	// Register client stub.
	client := pb.NewDiscoveryClient(conn)

	// Submit first request and obtain own ID
	response, err := client.NextCommand(context.Background(), &pb.SlaveStatus{
		SlaveId: -1,
		Status:  0,
		Message: "",
		Tag:     slaveTag,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("First request to master failed.")
	}
	ownSlaveID := response.Cmd.(*pb.MasterCommand_InitSlave).InitSlave.SlaveId
	logger.Info().Int32("ownSlaveId", ownSlaveID).Msg("Received own slave ID.")

	// Set wildcard replacements.
	wildcards := make(map[string]string)
	wildcards[discovery.WildcardSlaveID] = fmt.Sprintf("%0"+strconv.Itoa(numIDDigits)+"d", ownSlaveID)
	wildcards[discovery.WildcardPublicIP] = ownPublicIP
	wildcards[discovery.WildcardPrivateIP] = ownPrivateIP

	// Enter command execution loop: Ask master server for next command, execute it, ask for the next command, etc...
	// The request message (pb.SlaveStatus) contains the status of the last executed command.
	var exitStatus int32 = 0
	var exitMessage = "OK"
	var execCmd *exec.Cmd
	var execOutFile *os.File = nil
	var cmdID int32 = -1
cmdLoop:
	for {
		// Print status of last command.
		if exitStatus == 0 {
			logger.Info().Int32("status", exitStatus).Msg(exitMessage)
		} else {
			logger.Error().Int32("status", exitStatus).Msg(exitMessage)
		}

		// Submit request for command.
		nextCmd, err := client.NextCommand(context.Background(), &pb.SlaveStatus{
			CmdId:   cmdID,
			SlaveId: ownSlaveID,
			Status:  exitStatus,
			Message: exitMessage,
			Tag:     slaveTag})
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to get next command from the master.")
		} else {
			logger.Info().Int32("cmdId", nextCmd.CmdId).Msg("Received command.")
		}

		// Save command id for submitting it in the next command request.
		cmdID = nextCmd.CmdId

		// Execute command, depending on type.
		switch cmd := nextCmd.Cmd.(type) {

		// Start a program in the background as a separate process.
		case *pb.MasterCommand_ExecStart:
			logger.Info().
				Str("cmd", cmd.ExecStart.Name).
				Str("outFile", cmd.ExecStart.OutputFileName).
				Str("args", fmt.Sprint(cmd.ExecStart.Args)).
				Msg("Received an ExecStart command")
			if execCmd != nil { // Some command already running
				exitMessage = "Ignoring command (another command already running)."
				exitStatus = 1
			} else { // No command running yet

				// Replace wildcards in output file name and arguments by local values.
				// E.g., discovery.WildcardSlaveID (__id__ at the time of writing this comment)
				// is replaced by own slave ID.
				cmd.ExecStart.OutputFileName = replaceWildcards(cmd.ExecStart.OutputFileName, wildcards)
				for i, arg := range cmd.ExecStart.Args {
					cmd.ExecStart.Args[i] = replaceWildcards(arg, wildcards)
				}

				// Create Command to execute
				execCmd = exec.Command(cmd.ExecStart.Name, cmd.ExecStart.Args...)

				// Open output file (will be closed when executing the ExecWait command)
				if execOutFile, err = os.Create(cmd.ExecStart.OutputFileName); err != nil {
					logger.Error().
						Err(err).
						Str("outFileName", cmd.ExecStart.OutputFileName).
						Msg("Could not open file for writing")
					// Redirect Command's output to file
				} else {
					execCmd.Stdout = execOutFile
					execCmd.Stderr = execOutFile
				}

				// Launch Command
				if err = execCmd.Start(); err != nil {
					exitMessage = "Failed to start command: " + err.Error()
					exitStatus = 2
				} else {
					exitMessage = "OK"
					exitStatus = 0
				}
			}

		// Wait for program running in the background to finish.
		case *pb.MasterCommand_ExecWait:
			exitStatus = 0

			logger.Info().Str("cmdType", "ExecWait").Msg("Received command.")
			if execCmd == nil {
				exitMessage = "No program to wait for."
				exitStatus = 1
			} else {

				// Send a INT signal to the process after the timeout.
				// If that doesn not stop the process, we send the KILL signal after another another timeout.
				timerInt := time.AfterFunc(time.Millisecond*time.Duration(cmd.ExecWait.Timeout), func() {
					_ = execCmd.Process.Signal(os.Interrupt)
				})
				timerKill := time.AfterFunc(time.Millisecond*time.Duration(2*cmd.ExecWait.Timeout), func() {
					_ = execCmd.Process.Signal(os.Kill)
				})
				err := execCmd.Wait()
				if !timerInt.Stop() {
					exitMessage = fmt.Sprintf("Process interrupted after timeout of %d ms", cmd.ExecWait.Timeout)
					exitStatus = 2
				}
				if !timerKill.Stop() {
					exitMessage = fmt.Sprintf("Process KILLED after timeout of %d ms", 2*cmd.ExecWait.Timeout)
					exitStatus = 3
				}

				execCmd = nil

				if exitStatus == 0 && err != nil {
					exitMessage = "Error waiting for program: " + err.Error()
					exitStatus = 4
				} else if exitStatus == 0 {
					if err := execOutFile.Close(); err != nil {
						logger.Error().Err(err).Msg("Could not close output file.")
						exitMessage = "Could not close output file."
						exitStatus = 5
					} else {
						exitMessage = "OK"
					}
				}
			}

		// Send signal to program running in the background.
		// The process is expected to exit on ANY received signal.
		case *pb.MasterCommand_ExecSignal:
			logger.Info().Str("cmdType", "ExecSignal").Msg("Received command.")
			if execCmd == nil {
				exitMessage = "No program to send signal to."
				exitStatus = 1
			} else {
				var err error = nil
				switch cmd.ExecSignal.Signum {
				case pb.ExecSignal_DEFAULT:
					err = execCmd.Process.Signal(syscall.SIGINT)
				case pb.ExecSignal_SIGHUP:
					err = execCmd.Process.Signal(syscall.SIGHUP)
				case pb.ExecSignal_SIGINT:
					err = execCmd.Process.Signal(syscall.SIGINT)
				case pb.ExecSignal_SIGKILL:
					err = execCmd.Process.Signal(syscall.SIGKILL)
				case pb.ExecSignal_SIGUSR1:
					err = execCmd.Process.Signal(syscall.SIGUSR1)
				case pb.ExecSignal_SIGUSR2:
					err = execCmd.Process.Signal(syscall.SIGUSR2)
				case pb.ExecSignal_SIGTERM:
					err = execCmd.Process.Signal(syscall.SIGTERM)
				}
				if err != nil {
					exitMessage = "Error sending signal to process: " + err.Error()
					exitStatus = 2
				} else {
					// Assume process terminates after being sent the signal and wait collect it.
					execCmd.Wait()
					execCmd = nil

					if err := execOutFile.Close(); err != nil {
						logger.Error().Err(err).Msg("Could not close output file.")
						exitMessage = "Could not close output file."
						exitStatus = 3
					} else {
						exitMessage = "OK"
						exitStatus = 0
					}
				}
			}

		case *pb.MasterCommand_Noop:
			logger.Info().Str("cmdType", "Noop").Msg("Received command.")

			exitMessage = "Noop. Doing nothing."
			exitStatus = 0

		// Exit the command execution loop.
		case *pb.MasterCommand_Stop:
			logger.Info().Str("cmdType", "Stop").Msg("Received command.")
			break cmdLoop

		default:
			logger.Warn().Msg("Received unknown command.")

			exitStatus = 1
			exitMessage = fmt.Sprint("Unknown command:", cmd)
		}
	}
}

func replaceWildcards(data string, mapping map[string]string) string {
	for orig, repl := range mapping {
		// Could have used ReplaceAll, but reverted to this for compatibility with old Go versions.
		data = strings.Replace(data, orig, repl, -1)
	}
	return data
}
