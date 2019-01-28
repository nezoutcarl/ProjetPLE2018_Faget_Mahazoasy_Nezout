var express = require('express');
var router = express.Router();
var hbase = require('hbase');

//instantiate client 
const client = hbase({host: '10.0.5.25', port: 2181})

client.tables((error, tables) => {
  console.log('OKOK')
  console.info(tables)
})


/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

/* GET the map page */
router.get('/map', function(req, res, next) {
  res.render('map', { title: 'Map Page' });
});

module.exports = router;
