var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

/* GET the map page */
router.get('/map', function(req, res, next) {
  res.render('map', { title: 'Map Page' });
});

module.exports = router;
