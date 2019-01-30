#TODOS
# Make a way to force close a trade from the GSheet.
# User enters an exit price. If the trade is open in the DB but closed on the GSheet, place an order to market close all remaining in IB and update GSheet with close details.



DigitalOcean: IBController Install Info
---------------------------------------

# Sign into digital ocean droplet

# Go to downloads folder
cd /home/ubuntu/downloads

# Download IBController: 
wget https://github.com/ib-controller/ib-controller/releases/download/3.4.0/IBController-3.4.0.zip

# Unpack it to /opt
sudo unzip IBController-3.4.0.zip -d /opt/IBController

# Make user directory
mkdir /home/ubuntu/IBController


# Copy .ini file to user directory
sudo cp /opt/IBController/IBController.ini /home/ubuntu/IBController/IBController.ini

# Edit the config file
vim /home/ubuntu/IBController/IBController.ini

# Update config settings
	IbLoginId=
	IbPassword=
	TradingMode= live (or) paper
	ForceTwsApiPort = 4002 (if TradingMode = paper) or 4001
	AllowBlindTrading = yes
	DismissPasswordExpiryWarning = yes
	
