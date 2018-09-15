# GateWay:

This is part of larger IoT platform i made. The gateway itself only contains the parts moving and securing the data.
It will not work without the rest of the project, which i sadly cant upload online!

# The file explained:

the gateway is run though the DataGateway() function, and configuration is set in a seperate config file.

The gateway i designed to connect with multiple PLCs(Sequencial computers), and collect data to store in a database! The gateway itself is stored in a self contained unit(could be a small PC with no other purpos). It then connects to the PLCs though the GetPLCsToConnect() method.

The main system is comprised of 2 main components:

the Read- and the Write-module.
The read module is basicly a connection to every PLC set available through the configs.

# The read module:

The read module basicly reads the data from a PLC through network connection.
It then stores the the data in a Datasource object, consiting of:

the pulled data(any type)
the time of the pull
the type of the data

The read module then sends the package to a buffer, which contain the pulled from all the PLCs.
This then loops, and stores more and more data.
The reader is tested to read once every 0.2 seconds. and can handle up to 30k requests per PLC per minute.

# The write module:

The writer is picking the data off the buffer and placing it on the main external database.

the writer module build a data payload, which contain multiple datapoints in a larger chunk. Then ships it to a connecting server, where the data will be stored. This is validated, and a validation call is send back, to check that all data was placed correctly.

This utilize the ADL module which makes sure that no data is lost.

# Side notes:

The gateway, is build as a very universal approach, and all databases, and PLC types can be substituted. This means, the interfaces build on this project, can be used, by almost any PLC and database combination.

Supported Databases:

- Embedded LiteSQL DB

- External LiteSQL DB

- Azure cosmosDB

- MongoDB
 



supported PLCs:

- Beckenhoff TwinCat 3 systems

- S7 PLCs 
 
  

# The rest of the project

I can't upload the rest of the project online. But i would be able to send you details on how i made the rest, of the project. So send me an email, and I can give a tour of the software.
