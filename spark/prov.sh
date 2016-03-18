
# Install Spark Server
echo "-------> Provisioning"

echo "-------> Installing Oracle JRE"
sudo apt-get update
sudo apt-get install -y -q software-properties-common
sudo apt-get install -y -q python-software-properties
sudo apt-get update
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
sudo apt-get install -y -q oracle-java7-installer
echo "-------> Oracle Runtime JRE should be default"
java -version

echo "-------> Installation of commonly used python scipy tools"
sudo apt-get -y install python-numpy python-scipy python-matplotlib ipython ipython-notebook python-pandas python-sympy python-nose

echo "-------> Installation of scala"
wget http://www.scala-lang.org/files/archive/scala-2.11.1.deb
sudo dpkg -i scala-2.11.1.deb
sudo apt-get update
sudo apt-get -y -q install scala

echo "-------> Installation of sbt" 
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
sudo apt-get update
sudo apt-get install -y -q sbt

echo "-------> Downloading spark"
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.0.0.tgz
tar -zxf spark-1.0.0.tgz
cd spark-1.0.0

echo "-------> Building spark"
./sbt/sbt assembly

echo "-------> Clean-up"
rm scala-2.11.1.deb
rm sbt.deb
rm spark-1.0.0.tgz
rm install.sh

echo "-------> Spark installation done"
