sudo mkdir -p /opt/gopath/src/github.com/IBM/
sudo chown -R $(whoami):$(whoami)  /opt/gopath/
cd /opt/gopath/src/github.com/IBM/
git git@github.com:IBM/mirbft.git
cd /opt/gopath/src/github.com/IBM/
git checkout research