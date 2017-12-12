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

var platformList = ["GANNET A",
  "CAYLEY",
  "TEAL SOUTH",
  "SAXON",
  "GUILLEMOT NORTH",
  "GODWIN",
  "ENOCHDHU",
  "MONAN",
  "GADWALL",
  "MONTROSE",
  "BRIMMOND",
  "GANNET D",
  "DAUNTLESS",
  "GUILLEMOT A",
  "MARNOCK-SKUA (oil)",
  "ETTRICK",
  "CHESTNUT",
  "MALLARD",
  "HUNTINGTON",
  "GOLDEN EAGLE",
  "GANNET G",
  "ARBROATH",
  "ARKWRIGHT",
  "GANNET F",
  "HANNAY",
  "MAULE",
  "TWEEDSMUIR SOUTH",
  "ELGIN",
  "FLEMING",
  "NELSON",
  "CARNOUSTIE",
  "TEAL",
  "SCOLTY",
  "SCOTER (oil)",
  "SHELLEY",
  "KITTIWAKE",
  "EVEREST",
  "GANNET C",
  "BUZZARD",
  "MERGANSER",
  "BRODGAR",
  "CRATHES",
  "MARNOCK-SKUA (cond)",
  "BARDOLINO",
  "HAWKINS",
  "BRECHIN",
  "GUILLEMOT WEST",
  "FORTIES",
  "MIRREN",
  "COOK",
  "HOWE",
  "PICT",
  "WOOD",
  "HERON",
  "GUILLEMOT NORTH WEST",
  "DURWARD",
  "SCOTER (gas)",
  "GANNET E",
  "SHAW",
  "DRAKE",
  "CLAPHAM",
  "EGRET",
  "GOOSANDER",
  "GANNET B",
  "MADOES",
  "BUCHAN",
  "GROUSE",
  "TWEEDSMUIR",
  "SEYMOUR"
]
let jsonStream = StreamArray.make();

var jsonfile = "exp-data/voyages.json";

var stream = fs.createWriteStream("voyages2.csv");
stream.once('open', function(fd) {
  stream.write("id" + ",vesselName,vhash,companyName,chash,platformName,phash,voyageCount,multi\n");
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
  var voyageCount = voyage.locations.length;
  var firstCompanyName = voyage.locations[0].companyName;
  voyage.locations.forEach(function(platform) {
    if (platformList.indexOf(platform.name) > -1) {
      i++;
      var multi = 0;
      if (platform.companyName != firstCompanyName) {
        multi = 1
      };
      stream.write(i + "," + vessel.Name + ","+ vessel.Name.hashCode() + "," + platform.companyName + ","+ platform.companyName.hashCode()+ ","  + platform.name + ","+platform.name.hashCode() + ","  + voyageCount + "," + multi + "\n");
    }
  });


}
