/**
 * @Author: John Isaacs <john>
 * @Date:   20-Oct-172017
 * @Filename: processor.js
 * @Last modified by:   john
 * @Last modified time: 22-Nov-172017
 */



const StreamArray = require('stream-json/utils/StreamArray');
const path = require('path');
const fs = require('fs');
var i = 0;

var platformList = [
  "SCOTER (gas)",
  "SCOTER (oil)",
  "MERGANSER",
  "HERON",
  "EGRET",
  "MARNOCK-SKUA (oil)",
  "MARNOCK-SKUA (cond)",
  "BRECHIN",
  "ARKWRIGHT",
  "SHAW",
  "CARNOUSTIE",
  "ARBROATH",
  "WOOD",
  "MONTROSE",
  "GODWIN",
  "CAYLEY",
  "GANNET D",
  "GANNET G",
  "GANNET A",
  "GANNET B",
  "GANNET C",
  "GANNET E",
  "GUILLEMOT A",
  "GUILLEMOT WEST",
  "GUILLEMOT NORTH",
  "GUILLEMOT NORTH WEST",
  "CLAPHAM",
  "SAXON",
  "PICT",
  "TEAL",
  "TEAL SOUTH",
  "COOK",
  "MALLARD",
  "GADWALL",
  "GROUSE",
  "KITTIWAKE",
  "GOOSANDER",
  "CRATHES",
  "SCOLTY",
  "DAUNTLESS",
  "DURWARD",
  "BUCHAN",
  "TWEEDSMUIR SOUTH",
  "TWEEDSMUIR",
  "HANNAY",
  "ETTRICK",
  "GOLDEN EAGLE",
  "BUZZARD",
  "BRODGAR",
  "ENOCHDHU",
  "CHESTNUT",
  "SHELLEY",
  "EVEREST",
  "DRAKE",
  "FLEMING",
  "SEYMOUR",
  "HAWKINS",
  "HUNTINGTON",
  "BARDOLINO",
  "HOWE",
  "NELSON",
  "BRIMMOND",
  "FORTIES",
  "MAULE",
  "MONAN",
  "MIRREN",
  "MADOES",
  "ELGIN",
  "GANNET F"
]
let jsonStream = StreamArray.make();

var jsonfile = "exp-data/voyages.json";

var stream = fs.createWriteStream("voyages2.csv");
stream.once('open', function(fd) {
  stream.write("id" + ",vesselName,vhash,companyName,chash,platformName,phash,voyageCount,multi,sortid\n");
  fs.createReadStream(jsonfile).pipe(jsonStream.input);

});

//You'll get json objects here
jsonStream.output.on('data', function({
  index,
  value
}) {
  processRow(value);
});

jsonStream.output.on('end', function() {
  console.log('All done');
  stream.end();
});


String.prototype.hashCode = function() {
  var hash = 0, i, chr;
  if (this.length === 0) return hash;
  for (i = 0; i < this.length; i++) {
    chr   = this.charCodeAt(i);
    hash  = ((hash << 5) - hash) + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
};
function processRow(voyage) {

  var vessel = voyage.vessel;
  var voyageCount = voyage.locations.length;//multiple platfroms
  var firstCompanyName = voyage.locations[0].companyName;
  voyage.locations.forEach(function(platform) {
    var indexcheck = platformList.indexOf(platform.name);
    if ( indexcheck > -1) {
      i++;
      var multi = 0;
      if (platform.companyName != firstCompanyName) {
        multi = 1 //multiple comapnies
      };
      stream.write(i + "," + vessel.Name + ","+ vessel.Name.hashCode() + "," + platform.companyName + ","+ platform.companyName.hashCode()+ ","  + platform.name + ","+platform.name.hashCode() + ","  + voyageCount + "," + multi + "," + indexcheck +"\n");
    }
  });


}
