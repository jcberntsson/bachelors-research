
echo "Provisioning"

echo "Installing Oracle JRE"
sudo apt-get update
sudo apt-get install -y -q software-properties-common
sudo apt-get update
sudo apt-get install -y -q python-software-properties
sudo apt-get update
sudo add-apt-repository -y -q ppa:webupd8team/java
sudo apt-get update
sudo apt-get install -y -q oracle-java8-set-default
echo "Oracle Runtime JRE should be default"
java -version

sudo apt-get update
sudo apt-get install -y -q virtualbox

sudo apt-get update
sudo apt-get install -y -q vagrant
sudo apt-get update
sudo apt-get install -y -q virtualbox-dkms

sudo apt-get update
sudo apt-get install -y -q git

echo "Done"