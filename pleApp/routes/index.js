let express = require('express');
let router = express.Router();
let app = require('../app.js');
let path = require('path');
let hbase = require('hbase-rpc-client');

//instantiate client 
const client = hbase({
    zookeeperHosts: ['young'] ,
    zookeeperRoot: '/hbase'
});

/* GET home page. */
router.get('/', function(req, res, next) {
    res.render('index', { title: 'PLE Project App' });  
}); 
 
/* GET the map page */
router.get('/map', function(req, res, next) {  
  res.render('map', { title: 'Map Page' });
});

/* GET Specific image from HBASE TABLE DB */
router.get('/image/:z/:x/:y', (req, res, next) => {  
    if (req.params.x && req.params.y && req.params.z) {
      //let rowID = req.params.x +','+ (180-Number(req.params.y));
        let key = { "z": parseInt(req.params.z),
            "x":  parseInt(req.params.x),
            "y":  parseInt(req.params.y)};
    
        rowKey =key.z+'/'+key.y+'/'+key.x
        get = new hbase.Get(rowKey)
        key = { "z": parseInt(req.params.z),
            "x":  parseInt(req.params.x),
            "y":  parseInt(req.params.y)};
    
        client.get('famane', get,function (err,value1) { 
            if (err) {
                res.sendFile(path.join(__dirname, '../public/default.png'))
            }
            else {
                if(value1 !== null){                      
                    let val = value1.columns[0].value;
                    let data = new Buffer(val, 'base64');
                    res.contentType('image/jpeg');
                    res.send(data);
                }          
            }
               
        });          
    }
});

module.exports = router;
