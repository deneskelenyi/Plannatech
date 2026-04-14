A client written in Python for Plannatech's service of odds and match information.

The code is rather old and a bit of a spaghetti here and there. But you might get some usable bits out of it. 
There are some workarounds for fixing the service's incorrect usage of time stamps (Z at the end, but it's not ZULU time).

Use it as you will. It will work as it is, but the publisher is pointing to a service you will need to develop, so actually receive the data. 


WHY the local broker architecture?

This code was running on an AMD 3900X server with 128GB RAM and SSD driver. 
The database had a fairly complex structure, and it was impossible to handle the bursts of data coming in from LSports, since our subscription had over 30 providers' data in it.
So I wrote a client and a "shovel" process to queue updates the database couldn't process on time. 

Since this database was shared with Plannatech's database, it meant sense to cache this data into a local queue the same way.

Please feel free to contact me for implementating feeds into your system. 
No, the code will look much better this time. This was a very stressfull, constrained development process with the specification changing on a daily basis. So it's a bit of spaghetti right there. 

Feel free to check out my other repo for LSports. 

Unfortunately I do not have access anymore to Sportradar, Donbest (RIP) and Tip-ex codebases, but I am more than happy to work with any of those.



