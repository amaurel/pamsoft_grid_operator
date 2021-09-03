# pamsoft_grid_operator

```shell
docker build -t tercen/pamsoft_grid_operator .
```


##### Notes

Steps to install MCR in the rstudio image

1. Upload the MCR to /home/rstudio/mcr/ ($MCRTMP) in the tercen-studio image

2. Create an installer_input.txt in $MCRTMP with the following content:

```
mode silent
destinationFolder /home/rstudio/mcr
agreeToLicense yes
product.MATLAB_Runtime___Core true
product.MATLAB_Runtime___Numerics true
product.MATLAB_Runtime___Image_Processing_Toolbox_Addin true
product.MATLAB_Runtime___Statistics_and_Machine_Learning_Toolbox_Addin true
```

3. Install pre-requisites libs
```
sudo apt-get install libxtst6
```

4. Install the MCR files  (Note that the it is necessary to pass the absolute path to the installer input file)
```
cd ~/mcr/
chmod +x install
chmod +x -R bin/
chmod +x -R sys/java/jre/glnxa64/jre/bin/
./install -inputfile /home/rstudio/MCR/tmp/installer_input.txt
```
5. Upload the standalone pamsoft_grid files to /mcr/exe and set execute permission to them (chmod +x)


