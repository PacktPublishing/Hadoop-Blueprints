
var express = require("express");
var mysql = require('mysql');
var url = require('url');
var connection = mysql.createConnection({
    host: '192.168.2.112',
    user: 'root',
    password: '',
    database: 'customer360'
});
var app = express();

// app.set('view engine', 'jade');
// app.set('views', './views');

connection.connect(function(err) {
    if (!err) {
        console.log("Database is connected ... \n\n");
    } else {
        console.log("Error connecting database ... \n\n");
    }
});

app.get("/", function(req, res) {
    res.setHeader('Content-Type', 'text/html');
    res.writeHead(200);
    res.write("<h2>Get Customer 360 degree View</h2>")
    res.write("<form action=\"fetch\" method=\"get\">");
    res.write("Enter Customer Id  <input type=\"text\" name=\"custid\">");
    res.write("<input type=\"submit\" value=\"Submit\">");
    res.end("</form>");
});


app.get("/fetch", function(req, res) {
    var url = require('url');
    var queryData = url.parse(req.url, true).query;

    var sqlQuery = "SELECT * from  t_custbrandvisits where userid=\'" + queryData.custid.trim() + "\'";
    console.log(sqlQuery);
    connection.query(sqlQuery, function(err, results) {
        res.setHeader('Content-Type', 'text/html');
        res.writeHead(200);
        if (results.length > 0) {

            res.write("<table border=\"1\" style=\"width:80%\">");
            res.write("<tr><th colspan=\"2\"><h2>Customer 360 Degree View </h2></th></tr>");
            res.write("<tr><th colspan=\"2\">");
            res.write("User ID: " + results[0].userid + 
                      " User Name: " + results[0].first_name + " " + results[0].last_name +
                      " Gender: " +  results[0].gender +
                      " Email: " +  results[0].email);
            res.write("</th></tr>");
          
            for (var i = 0; i < results.length; i++) {
                
                res.write("<tr><td>");
                res.write(results[i].brand);
                res.write("</td><td>");
                res.write("Visits: "+ results[i].cnt);
                res.write("</td></tr>");
            }
            res.write("</table>");
            
        } else {
            res.write('No matching brand data for ' + queryData.custid.trim());
        }

        var sqlQuery = "SELECT * from  t_usertweets where userid=\'" + queryData.custid.trim() + "\' order by followercnt desc";
        
        console.log(sqlQuery);
        
        connection.query(sqlQuery, function(err, results) {
        
        if (results.length > 0) {
            
            res.write("<table border=\"1\" style=\"width:80%\">");
            res.write("<tr><th>Latest Tweets from Customer</th></tr>");
            res.write("<tr><th>");
            res.write("Followers: "+ results[0].followercnt );
            
            if (results[0].followercnt > 1000)
               res.write("  VIP Customer" );
            
            res.write("</th></tr>");
            
            for (var i = 0; i < results.length; i++) {
                
                res.write("<tr><td>");
                res.write(results[i].tweettext);
                res.write("</td></tr>");
            }
            res.write("</table>");
           
        } else {
            res.write('No matching tweet data for ' + queryData.custid.trim() + '<BR>');
        }
        res.end("");
      });

        if (err) {
            console.log('Error while performing Query.');
        }
    });


});

app.get("/end", function(req, res) {

    res.setHeader('Content-Type', 'text/html');
    res.writeHead(200);
    res.end("Session Ended");
    connection.end();
});
 
app.listen(3000);

console.log("Started the web server on port 3000");