const fs = require('fs');
const csv = require('fast-csv');
const geolib = require('geolib');
const third_district_coordinates = require('./data/3rdDistrictCoordinates');
const readStream = fs.createReadStream('./data/city_of_long_beach.csv');
const writeStream = fs.createWriteStream('./data/output.json');

// however many rows processed you'd like an update
const logEveryX = 1000;

// variable to track rows processed
let rowCount = 0;

// start JSON stream with array bracket
let separator = '[';

// check if address is within district coordinates
const isIn3rdDistrict = (latitude, longitude) => {
  return geolib.isPointInPolygon(
    { latitude, longitude },
    third_district_coordinates
  );
}

// remove empty string fields in object
const cleanAddr = (addrObj) => {
  Object.keys(addrObj).forEach(
    (key) => {
      if (addrObj[key] === "") {
        delete addrObj[key];
      }
    }
  )
  return addrObj;
}

// filter out third district rows
const handleCsvData = (row) => {
  const { LAT, LON } = row;
  const inThird = isIn3rdDistrict(LAT, LON);

  if (!inThird) return;
  
  // remove empty fields for filesize
  row = cleanAddr(row);

  writeStream.write(
    separator + JSON.stringify(row, null, 3)
  );
  
  // after first row, comma-separate JSON
  separator = ',';
  
  // log every X rows
  if (rowCount % logEveryX === 0) {
    console.log(`parsing rows ${rowCount} - ${rowCount+logEveryX}...`);
  }
  rowCount++;
}

readStream
  .pipe(csv.parse({ headers: true }))
  .on('data', handleCsvData)
  .on('end', () => {
    // close JSON stream with array bracket
    writeStream.write("]");
  });