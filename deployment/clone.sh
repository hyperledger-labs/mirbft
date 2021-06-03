sudo mkdir -p /opt/gopath/src/github.com/IBM/
sudo chown -R $(whoami):$(whoami)  /opt/gopath/
cd /opt/gopath/src/github.com/IBM/
git clone git@github.com:hyperledger-labs/mirbft.git
cd /opt/gopath/src/github.com/IBM/
git checkout research
