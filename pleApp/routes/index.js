let express = require('express');
let router = express.Router();
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
router.get('/image/:z/:y/:x', (req, res, next) => {  
    if (req.params.x && req.params.y && req.params.z) {

      //let rowID = req.params.x +','+ (180-Number(req.params.y));
        let key = { "z": 1,

            "y": 180 - Number(req.params.y),
            "x": Number(req.params.x)};

        rowKey =key.z+'/'+key.y+'/'+key.x 
        console.log(rowKey);
        //rowKey= '1/170/200'
        get = new hbase.Get(rowKey)
    
        client.get('famane1201_seq', get,function(error,value1) { 
            
           try {
            if(value1 !== null){                      
                let val = value1.columns[0].value;
                console.log(val);
                let data = Buffer.from(val, 'base64');
                res.contentType('image/png');
                res.status(200).send(val);
            }
            else {
                res.status(400).sendFile(path.join(__dirname, '../public/default.jpg'));        
            }          
           } catch (error) {
               
           }            
            
               
        });          
    }
});

module.exports = router;
