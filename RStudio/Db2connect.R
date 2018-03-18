library(ibmdbR)
host.name <- "172.17.0.1"
user.name <-"bluadmin"
pwd <- "bluadmin"
######################################################################

con <- idaConnect(paste("DASHDB",";Database=BLUDB;Hostname=",host.name,";Port=50000;PROTOCOL=TCPIP;UID=", user.name,";PWD=",pwd,sep=""),"","") 
idaInit(con)

#Show tables and views in the user schema
idaShowTables()

idaClose(con)

