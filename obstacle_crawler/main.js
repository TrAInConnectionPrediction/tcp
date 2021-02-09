// Read Config
console.log("Reading config...")
require('dotenv').config({ path: '/mnt/config/config.py' })
console.log("Done")
console.log(process.env.db_server)
console.log("Connecting to DB...")
const { Pool, Client } = require('pg')
const client = new Client({
    user: process.env.db_username,
    host: process.env.db_server,
    database: process.env.db_database,
    password: process.env.db_password,
    port: 5432,
})
client.connect();
console.log("Done")


// client.query(`DROP TABLE obstacle`, (err, res) => {
//     console.log(err, res)
// })

// client.query(`CREATE TABLE obstacle(                                                        
//     id VARCHAR(20) PRIMARY KEY NOT NULL,
//     data JSONB NOT NULL 
//     );`, (err, res) => {
//     console.log(err, res)
// })

async function get(id) {
    const query_return = await client.query(
        `
        SELECT data
        FROM obstacle
        WHERE id=$1;
      `, [id]).then(res => {
        console.log(res.rows[0])
    }).catch(e => console.error(e.stack));
    return query_return.rows;
}

async function set(id, value) {
    return await client.query(`
      INSERT INTO obstacle (id, data)
      VALUES ($1, $2)
      ON CONFLICT (id)
      DO UPDATE 
      SET data = excluded.data;
    `, [id, value]).then(res => {
        console.log(res.rows[0])
    }).catch(e => console.error(e.stack));
}


const createClient = require('db-netz-hafas')
const crawlerClient = createClient('db-netz-hafas client')

setInterval(function() {
    // I promise you I didn't understand this at first https://www.youtube.com/watch?v=s6SH72uAn3Q
    crawlerClient.remarks({
        north: 52.643063,
        west: 12.943267,
        south: 52.354634,
        east: 13.822174
    }).then((data) => {
        var obstacle;
        for (obstacle of data) {
            console.log(obstacle.id)
            set(obstacle.id, obstacle);
        }
    }, console.error)
}, 1000 * 60)


// client.query(`SELECT * FROM obstacle`).then(text => {
//     console.log("Promise resolved " + text);
// }).catch(text => {
//     console.log("Promise Rejected " + text);
// });