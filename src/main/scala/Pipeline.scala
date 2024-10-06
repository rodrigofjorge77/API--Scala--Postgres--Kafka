import sttp.client3._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import io.circe._
import io.circe.parser._
import scala.io.Source
import java.sql.{Connection, DriverManager, SQLException}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import java.util.Properties
import scala.collection.mutable.ListBuffer

@main def apiRequest(): Unit =
  // Create a basic request object using sttp
  val backend = HttpURLConnectionBackend()

  // Define the URL of the API
  val url = "https://data.cityofnewyork.us/resource/k397-673e.json" // Replace with your actual API URL
      
    // Criar um ListBuffer mutável para armazenar os valores
  val buffer = ListBuffer[String]()

  // Make the GET request and get the response
  val request = basicRequest.get(uri"$url")

  // Send the request asynchronously
  val response: Future[Response[Either[String, String]]] = Future {
    request.send(backend)
  }

  // Handle the response asynchronously
  response.onComplete {
    case Success(response) =>
      response.body match {
        case Right(responseBody) =>
          //println(s"Success! The response body is: $responseBody") // Store or use the response body as needed

          val resp = responseBody.replace("[","").replace("]","")
          //resp  = resp.replace("]","")

          val path = Paths.get("output.json")

          //Write the JSON string to the file
          Files.write(path, resp.getBytes(StandardCharsets.UTF_8))

          //println(s"JSON has been written to ${path.toAbsolutePath}")          

          // Step 1: Open the file using Source.fromFile
          val filePath = "output.json"
          val fileSource = Source.fromFile(filePath)
          var isFirstLine = true

          //---------------------------------------------------------------------------
          // Configurações de conexão
          val url = "jdbc:postgresql://172.27.213.163:5433/postgres"
          val user = "postgres"
          val password = "senha"

          var connection: Connection = null


          try {
            // Passo 1: Conectar ao banco de dados
            connection = DriverManager.getConnection(url, user, password)
            connection.setAutoCommit(false) // Usar transações

            // Passo 2: Dropar a tabela se existir
            val dropTableSQL = "DROP TABLE IF EXISTS public.employees"
            val stmt = connection.createStatement()
            stmt.executeUpdate(dropTableSQL)

              val createTableSQL =
              """
              |CREATE TABLE public.employees (
              |  id int8 DEFAULT nextval('employees_seq'::regclass) NOT NULL,
              |  fiscal_year int4 NULL,
              |  payroll_number int4 NULL,
              |  agency_name varchar NULL,
              |  last_name varchar NULL,
              |  first_name varchar NULL,
              |  mid_init varchar NULL,
              |  agency_start_date varchar NULL,
              |  work_location_borough varchar NULL,
              |  title_description varchar NULL,
              |  leave_status_as_of_june_30 varchar NULL,
              |  base_salary numeric NULL,
              |  pay_basis varchar NULL,
              |  regular_hours numeric NULL,
              |  regular_gross_paid numeric NULL,
              |  total_ot_paid numeric NULL,
              |  total_other_pay numeric NULL,
              |  CONSTRAINT employees_pk PRIMARY KEY (id)
              |)
              """.stripMargin

            stmt.executeUpdate(createTableSQL)


            try {

              // Step 2: Iterate through each line
              for (line <- fileSource.getLines()) {
                                          
                // Step 3: Process each line (here we just print it)
                //println(line)

                var lineRead = ""

                if (isFirstLine) 
                {
                  // Ação específica para a primeira linha
                  //println(s"Primeira linha: $line")
                  isFirstLine = false  // Atualiza a flag para as próximas iterações                
                  lineRead = line
                  //println(s"Linha pased: $parsedJson")
                } 
                else 
                {                
                  lineRead = line.tail         
                  //println(s"Linha pased: $parsedJson")
                }
                
                // Passo 2: Analisar (parse) a string JSON
                var parsedJson = parse(lineRead)   
                var fiscal_year = "0"
                var payroll_number = "0"
                var agency_name = ""
                var last_name = ""
                var first_name = ""
                var mid_init = ""
                var agency_start_date = ""
                var work_location_borough = ""
                var title_description = ""
                var leave_status_as_of_june_30 = ""
                var base_salary = "0"
                var pay_basis = ""
                var regular_hours = "0"
                var regular_gross_paid = "0"
                var total_ot_paid = "0"
                var total_other_pay = "0"

                //println(lineRead)             

                // Passo 3: Manipular o resultado da análise e extrair as chaves e valores
                parsedJson match {
                  case Left(error) =>
                    println(s"Erro ao analisar JSON: $error")

                  case Right(json) =>
                    // Verificar se o JSON é um objeto e extrair as chaves e valores
                    json.asObject match {
                      case Some(jsonObject) =>
                        jsonObject.toMap.foreach { case (key, value) =>
                          println(s"Chave: $key, Valor: $value")

                          if(key == "fiscal_year")
                          {
                            fiscal_year = value.toString.replace("\"", "")
                            //fiscal_year = fiscal_year.replace("\"", "")
                          }
                          else if(key == "payroll_number")
                          {
                            payroll_number = value.toString.replace("\"", "")
                          }
                          else if(key == "agency_name")
                          {
                            agency_name = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "last_name")
                          {
                            last_name = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "first_name")
                          {
                            first_name = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "mid_init")
                          {
                            mid_init = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "agency_start_date")
                          {
                            agency_start_date = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "work_location_borough")
                          {
                            work_location_borough = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "title_description")
                          {
                            title_description = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "leave_status_as_of_june_30")
                          {
                            leave_status_as_of_june_30 = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "base_salary")
                          {
                            base_salary = value.toString.replace("\"", "")
                          }
                          else if(key == "pay_basis")
                          {
                            pay_basis = value.toString.replace("\"", "").replace("'", "")
                          }
                          else if(key == "regular_hours")
                          {
                            regular_hours = value.toString.replace("\"", "")
                          }
                          else if(key == "regular_gross_paid")
                          {
                            regular_gross_paid = value.toString.replace("\"", "")
                          }
                          else if(key == "total_ot_paid")
                          {
                            total_ot_paid = value.toString.replace("\"", "")                            
                          }
                          else if(key == "total_other_pay")
                          {
                            total_other_pay = value.toString.replace("\"", "")
                          }

                        }

                      case None =>
                        println("O JSON analisado não é um objeto válido.")
                    }

                }

                  // Passo 4: Inserir dados na tabela
                  var insertSQL = "INSERT INTO public.employees (fiscal_year, payroll_number, agency_name, last_name, first_name, mid_init, agency_start_date, work_location_borough, title_description, leave_status_as_of_june_30, base_salary, pay_basis, regular_hours, regular_gross_paid, total_ot_paid, total_other_pay ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                  var preparedStatement = connection.prepareStatement(insertSQL)
                  
                  buffer += title_description + "|" + total_ot_paid
                  preparedStatement.setInt(1, fiscal_year.toInt)                //fiscal_year
                  preparedStatement.setInt(2, payroll_number.toInt)                               //payroll_number
                  preparedStatement.setString(3, agency_name)                 //agency_name
                  preparedStatement.setString(4, last_name)                   //last_name
                  preparedStatement.setString(5, first_name)                  //first_name
                  preparedStatement.setString(6, mid_init)                    //mid_init
                  preparedStatement.setString(7, agency_start_date)           //agency_start_date
                  preparedStatement.setString(8, work_location_borough)       //work_location_borough
                  preparedStatement.setString(9, title_description)           //title_description
                  preparedStatement.setString(10, leave_status_as_of_june_30)  //leave_status_as_of_june_30
                  preparedStatement.setFloat(11, base_salary.toFloat )         //base_salary
                  preparedStatement.setString(12, pay_basis)                  //pay_basis
                  preparedStatement.setFloat(13, regular_hours.toFloat)        //regular_hours
                  preparedStatement.setFloat(14, regular_gross_paid.toFloat)   //regular_gross_paid
                  preparedStatement.setFloat(15, total_ot_paid.toFloat)        //total_ot_paid
                  preparedStatement.setFloat(16, total_other_pay.toFloat)      //total_other_pay
                  
                  preparedStatement.executeUpdate()    
                  connection.commit()   
              }

            } finally {
              connection.commit()
              // Step 4: Close the file to free up resources
              fileSource.close()
            }                                  

          } catch {
            case e: SQLException =>
              e.printStackTrace()
              if (connection != null) {
                connection.rollback() // Reverter transações em caso de erro
              }
          } finally {
            // Fechar a conexão
            if (connection != null) {
              connection.close()
            }
          }

        case Left(error) =>
          println(s"Error: $error") // Handle the error case
      }

    case Failure(exception) =>
      println(s"Request failed with exception: $exception")
  }

  // Keep the program running to await the async response
  Thread.sleep(5000)   

  // Configuração do produtor Kafka
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.27.213.163:9092") // Servidor do Kafka
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")          

  // Criar um produtor Kafka
  var producer = new KafkaProducer[String, String](props)
  // Enviar uma mensagem para o tópico
  val topic = "testKafka"
        
  //println("Dados inseridos com sucesso.")                  

  // Converter o ListBuffer em uma lista (opcional, se quiser torná-la imutável)
  val listaFinal: List[String] = buffer.toList
  var reg = 0

  // Iterar sobre a lista para recuperar os valores
  for (valor <- listaFinal) {
    
    Thread.sleep(10)

    reg += 1
    //println(s"Valor: $valor")
    
    var keyKafka = "title_description|total_ot_paid"
    //var valueKafka = reg.toString + "-" + valor 

    var minhaString  = valor
    var itemGrafico  = minhaString.takeWhile(_ != '|')
    var valorGrafico = minhaString.dropWhile(_ != '|').drop(1)  // Dropa o caractere '|'
    
    var valueKafka = "{\"item\": \"" + itemGrafico + "\", \"value\":" + valorGrafico + "}"
  
    // Criação de um ProducerRecord (tópico, chave, valor)
    var record = new ProducerRecord[String, String](topic, keyKafka, valueKafka)
    // Envio da mensagem para o Kafka
    producer.send(record)  

  }

// Fechar o produtor para liberar recursos
  producer.close()