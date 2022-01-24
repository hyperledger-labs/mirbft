. vars.sh

export PATH=$PATH:~/go/bin/:/opt/gopath/bin/
export GOPATH=/opt/gopath
export GOROOT=~/go
export GO111MODULE=off

sudo mkdir -p /opt/gopath/src/github.com/IBM/
sudo chown -R $user:$group  /opt/gopath/
cd /opt/gopath/src/github.com/IBM/
if [ ! -d "/opt/gopath/src/github.com/IBM/mirbft" ]; then
  git clone https://github.com/hyperledger-labs/mirbft.git
fi
cd /opt/gopath/src/github.com/IBM/mirbft
git checkout research
git pull
./run-protoc.sh
cd /opt/gopath/src/github.com/IBM/mirbft/server
go build
cd /opt/gopath/src/github.com/IBM/mirbft/client
go build