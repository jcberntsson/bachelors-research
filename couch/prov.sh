
# Install CouchDB Server
echo "Provisioning"

echo "Installing VIM"
sudo apt-get update
sudo apt-get install -y -q vim
echo "VIM installed"

echo "Installing couchdb"
sudo apt-get update
sudo apt-get install -y -q couchdb
sed -i 's/^;port/port/' /etc/couchdb/local.ini
sed -i 's/^;bind_address = 127.0.0.1/bind_address = 192.168.33.15/' /etc/couchdb/local.ini
sudo service couchdb status
sudo service couchdb force-reload
sudo service couchdb status
echo "Couchdb installed"

sudo reboot

echo "Provisioning done"
