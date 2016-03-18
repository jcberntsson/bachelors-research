
# Install MySQL Server
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

sudo -i
wget -O - http://debian.neo4j.org/neotechnology.gpg.key | apt-key add -
echo 'deb http://debian.neo4j.org/repo stable/' > /etc/apt/sources.list.d/neo4j.list
apt-get update
apt-get install -y neo4j
service neo4j-service status

echo "Port 7474 should be running"
netstat -ntlp | grep LISTEN

echo "Changing neo4j settings"
sudo sed -i "s/^#org.neo4j.server.webserver.address/org.neo4j.server.webserver.address/" /etc/neo4j/neo4j-server.properties
sudo sed -i "s/^dbms.security.auth_enabled=true/dbms.security.auth_enabled=false/" /etc/neo4j/neo4j-server.properties

echo 'root   soft     nofile  40000' | sudo tee -a /etc/security/limits.conf
echo 'root   hard     nofile  40000' | sudo tee -a /etc/security/limits.conf
echo 'vagrant   soft     nofile  40000' | sudo tee -a /etc/security/limits.conf
echo 'vagrant   hard     nofile  40000' | sudo tee -a /etc/security/limits.conf
echo 'neo4j   soft     nofile  40000' | sudo tee -a /etc/security/limits.conf
echo 'neo4j   hard     nofile  40000' | sudo tee -a /etc/security/limits.conf

echo 'session    required   pam_limits.so' | sudo tee -a /etc/pam.d/su

echo "Changing IP table"
sudo iptables -I INPUT -p tcp --dport 7474 -j ACCEPT
sudo iptables-save

echo "Restarting neo4j"
sudo service neo4j-service restart

echo "Neo4j is now available at 192.168.33.13:7474"
echo "Machine available for ssh at 127.0.0.1:12917"

echo "Provisioning done, rebooting"

sudo reboot
