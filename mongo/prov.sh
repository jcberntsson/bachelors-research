
# Install MongoDB Server
echo "Provisioning"

echo "Install mongo"
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb.list
sudo apt-get update
sudo apt-get install mongodb-org

echo "Fixing mongo binding"
sed -i "/bindIp/d" /etc/mongod.conf
sudo service mongod restart

echo "Mongo Version"
mongod --version

echo "Done provisioning"