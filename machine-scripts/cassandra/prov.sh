
# Install Cassandra Server
echo "Provisioning"

echo "Installing VIM"
sudo apt-get update
sudo apt-get install -y -q vim
echo "VIM installed"

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

echo "Installing Cassandra"
echo "deb http://www.apache.org/dist/cassandra/debian 22x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
echo "deb-src http://www.apache.org/dist/cassandra/debian 22x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
echo "Adding three public keys"
gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
gpg --export --armor F758CE318D77295D | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
gpg --export --armor 2B5C1B00 | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 0353B12C
gpg --export --armor 0353B12C | sudo apt-key add -
echo "Keys inserted"
sudo apt-get update
sudo apt-get install -y cassandra
echo "Making sure Cassandra can start"
sed -i 's/^CMD_PATT="Dcassandra-pidfile=.*cassandra\.pid"/CMD_PATT="cassandra"/' /etc/init.d/cassandra
sudo rm -r /var/lib/cassandra
sudo rm -r /var/log/cassandra
sudo service cassandra stop
sudo service cassandra start
sudo service cassandra status
echo "Cassandra installation done"



