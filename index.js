require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const mysql = require('mysql');
const mqtt = require('mqtt');
const cors = require('cors');

const app = express();
app.use(bodyParser.json());
app.use(cors());  

// Configuración de la base de datos
const db = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
});

db.connect((err) => {
    if (err) {
        console.error('Error al conectar a la base de datos:', err);
        return;
    }
    console.log('Conectado a la base de datos');
});

// Configuración MQTT
const mqttClient = mqtt.connect(process.env.MQTT_BROKER);
const documento = '456789123';

// Tópicos MQTT
const tempCorporalTopic = `Monitoreo/${documento}/tempCorporal`;
const ritmoCardiacoTopic = `Monitoreo/${documento}/RitmoCardiaco`;
const saturacionOxigenoTopic = `Monitoreo/${documento}/SaturacionOxigeno`;
const tempAmbienteTopic = `Monitoreo/${documento}/TempAmbiente`;
const humedadAmbienteTopic = `Monitoreo/${documento}/HumedadAmbiente`;

// Mapeo de métricas según la descripción
const metricas = {
    tempCorporal: 1,
    ritmoCardiaco: 2,
    saturacionOxigeno: 3,
    tempAmbiente: 4,
    humedadAmbiente: 5
};

// Función para obtener el ID del paciente según el documento
function getPacienteID(documento) {
  return new Promise((resolve, reject) => {
      const sql = 'SELECT IdPaciente FROM Paciente WHERE documento = ?';
      console.log('Ejecutando consulta para obtener IDPaciente con documento:', documento);
      db.query(sql, [documento], (err, results) => {
          if (err) {
              console.error('Error al ejecutar la consulta para obtener el ID del paciente:', err);
              return reject(err);
          }

          // Verifica si la consulta devuelve resultados
          if (results.length === 0) {
              console.error(`No se encontró paciente con el documento: ${documento}`);
              return reject(new Error('Paciente no encontrado'));
          }

          // Si la consulta tuvo éxito, devuelve el ID del paciente
          console.log('IDPaciente encontrado:', results[0].IdPaciente);
          resolve(results[0].IdPaciente);
      });
  });
}

// Función para obtener el estado del paciente desde la tabla `Estados`
function getEstadoPaciente() {
  return new Promise((resolve, reject) => {
      const sql = 'SELECT IdEstado FROM Estados LIMIT 1'; // Selecciona un estado predeterminado
      console.log('Ejecutando consulta para obtener el estado del paciente');
      db.query(sql, (err, results) => {
          if (err) {
              console.error('Error al ejecutar la consulta para obtener el estado:', err);
              return reject(err);
          }

          // Verifica si se encuentra un estado
          if (results.length === 0) {
              console.error('No se encontraron estados en la tabla Estados');
              return reject(new Error('No hay estados disponibles'));
          }

          console.log('Estado encontrado:', results[0].IdEstado);
          resolve(results[0].IdEstado); // Devuelve el ID del estado que se usará
      });
  });
}

// Conexión al broker MQTT
mqttClient.on('connect', async () => {
  console.log('Conectado al broker MQTT');

  try {
      // Verifica que el documento esté siendo pasado correctamente
      console.log('Documento del paciente:', documento);
      
      // Obtener el ID del paciente con el número de documento
      const IDPaciente = await getPacienteID(documento);
      console.log(`IDPaciente obtenido: ${IDPaciente}`);

      // Obtener el estado del paciente
      const estado = await getEstadoPaciente();
      console.log(`Estado obtenido: ${estado}`);

      mqttClient.subscribe([
          tempCorporalTopic,
          ritmoCardiacoTopic,
          saturacionOxigenoTopic,
          tempAmbienteTopic,
          humedadAmbienteTopic
      ], (err) => {
          if (err) {
              console.error('Error al suscribirse a los tópicos:', err);
          } else {
              console.log('Suscripción exitosa a los tópicos MQTT');
          }
      });

      // Manejo de mensajes MQTT
      mqttClient.on('message', (topic, message) => {
          const value = parseFloat(message.toString());
          console.log(`Mensaje recibido del tópico ${topic}: ${value}`);

          let metrica = null;

          if (isNaN(value)) {
              console.log('Valor no válido recibido:', message.toString());
              return; // Si el valor no es válido, no hacer nada
          }

          if (topic === tempCorporalTopic) metrica = metricas.tempCorporal;
          if (topic === ritmoCardiacoTopic) metrica = metricas.ritmoCardiaco;
          if (topic === saturacionOxigenoTopic) metrica = metricas.saturacionOxigeno;
          if (topic === tempAmbienteTopic) metrica = metricas.tempAmbiente;
          if (topic === humedadAmbienteTopic) metrica = metricas.humedadAmbiente;

          if (metrica) {
              const sql = `
                  INSERT INTO DataOrigen (IDPaciente, Metrica, medidaValor, FechaHora, Estado)
                  VALUES (?, ?, ?, NOW(), ?)
              `;
              console.log('Ejecutando consulta:', sql, [IDPaciente, metrica, value, estado]);
              db.query(sql, [IDPaciente, metrica, value, estado], (err, result) => {
                  if (err) {
                      console.error('Error al insertar datos en DataOrigen:', err);
                  } else {
                      console.log('Datos insertados correctamente en DataOrigen:', result);
                  }
              });
          }
      });
  } catch (error) {
      console.error('Error al obtener el ID del paciente o el estado:', error);
  }
});

// Endpoint para consultar los datos ingresados en DataOrigen
app.get('/data', (req, res) => {
    const sql = 'SELECT * FROM DataOrigen ORDER BY IdData DESC LIMIT  1';
    db.query(sql, (err, results) => {
        if (err) {
            console.error('Error al obtener los datos:', err);
            return res.status(500).send('Error al obtener los datos');
        }
        res.json(results);
    });
});

// Endpoint para insertar datos manualmente (para pruebas)
app.post('/insertData', (req, res) => {
    const { idPaciente, metrica, medidaValor, estado } = req.body;
    const sql = `
        INSERT INTO DataOrigen (IDPaciente, Metrica, medidaValor, FechaHora, Estado)
        VALUES (?, ?, ?, NOW(), ?)
    `;
    console.log('Ejecutando consulta manual:', sql, [idPaciente, metrica, medidaValor, estado]);
    db.query(sql, [idPaciente, metrica, medidaValor, estado], (err, result) => {
        if (err) {
            console.error('Error al insertar datos:', err);
            return res.status(500).send('Error al insertar datos');
        }
        res.send('Datos insertados correctamente');
    });
});

// Iniciar servidor
app.listen(8000, () => {
    console.log('Servidor API en ejecución en http://localhost:8000');
});
