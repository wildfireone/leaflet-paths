
const StreamArray = require('stream-json/utils/StreamArray');
const path = require('path');
const fs = require('fs');
var i=0;
let jsonStream = StreamArray.make();

var jsonfile  = "exp-data/voyages.json";


var stream = fs.createWriteStream("voyages.csv");
stream.once('open', function(fd) {
stream.write("id"+",vesselName,companyName,platformName,voyageCount,multi\n");
	let filename = path.join(__dirname, 'sample.json');
	fs.createReadStream(jsonfile).pipe(jsonStream.input);

});

//You'll get json objects here
jsonStream.output.on('data', function ({index, value}) {
	processRow(value);
});

jsonStream.output.on('end', function () {
	console.log('All done');
  stream.end();
});



function processRow(voyage){

		var vessel = voyage.vessel;
		var voyageCount = voyage.locations.length;
		var firstCompanyName = voyage.locations[0].companyName;
		voyage.locations.forEach(function(platform){
			i++;
			var multi = 0;
			if(platform.companyName == firstCompanyName){multi = 1};
			stream.write(i+","+vessel.Name+ ","+platform.companyName+","+platform.name+","+voyageCount+","+multi+"\n");
		});


}
