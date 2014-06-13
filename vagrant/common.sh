# common.sh

# ------------------------------------------------
# update - and install pip
# ------------------------------------------------										

update() {
	echo ""
	echo "Updating..."
	echo ""
	sudo apt-get update --fix-missing
	
	echo ""
	echo "Done updating"
	echo ""
}

update