using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
// use mysql connector
using MySql.Data;
using MySql.Data.MySqlClient;
// use jsonconverter
using Newtonsoft.Json;
// use configurationbuilder
using Microsoft.Extensions.Configuration;

namespace calcularTributos
{
    internal class Tributo {
        public string? EmpresaId { get; set; } // prestador_id
        public string? RazaoSocialEmpresa { get; set; }
        public string? TributoNome { get; set; }
        public string? TributoValor { get; set; }
    }

    internal class Nota {
        public long NotaId { get; set; }
        public long PrestadorId { get; set; }
        public long TomadorId { get; set; }
        public string? RazaoSocialPrestador { get; set; }
        public string? RazaoSocialTomador { get; set; }
        public decimal Valor { get; set; }
    }

    public class calcularTributos
    {
        private readonly ILogger _logger;

        public calcularTributos(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<calcularTributos>();
        }

        [Function("calcularTributos")]
        public async Task<HttpResponseData> RunAsync([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            // create a new HTTP client object
            HttpClient client = new HttpClient();
            // call the API and get the response
            HttpResponseMessage responseMessage = await client.GetAsync("https://iob-smart-webapi.azurewebsites.net/api/Notas");
            // check if the response is a success
            if (!responseMessage.IsSuccessStatusCode)
            {
                // if not, return the status code
                return req.CreateResponse(responseMessage.StatusCode);
            } 
            // if it is, log the response
            _logger.LogInformation(responseMessage.ToString());

            // insert the response into a mysql database
            // create a new mysql connection object with the connection string from the environment variables
            MySqlConnection connection = new MySqlConnection(Environment.GetEnvironmentVariable("MYSQLCONNSTR_ConnectionString"));
            
            // open the connection
            connection.Open();
            // check if the connection is open
            if (connection == null) {
                // if not, log the error
                _logger.LogError("Connection to PlanetScale failed to open");
            } else {
                // if it is, log the connection
                _logger.LogInformation("Connection to PlanetScale opened");
                
                //responseMessage.EnsureSuccessStatusCode();
                // calculate aggregate valor for each empresa
                // get the response body as a string
                string responseBody = await responseMessage.Content.ReadAsStringAsync();
                // parse the response body into a list of Nota objects
                List<Nota> notas = JsonConvert.DeserializeObject<List<Nota>>(responseBody);
                // print the response body
                _logger.LogInformation(responseBody);
                // print the number of notas in the response
                _logger.LogInformation(notas.Count.ToString());
                _logger.LogInformation("Calculating aggregate tax for each company");
                // create a new list of Tributo objects
                List<Tributo> tributos = new List<Tributo>();
                // for each nota in the response
                foreach (var nota in notas) {
                    // create a new Tributo object
                    Tributo tributo = new Tributo();
                    // set the EmpresaId property to the PrestadorId property
                    tributo.EmpresaId = nota.PrestadorId.ToString();
                    // set the RazaoSocialEmpresa property to the RazaoSocialPrestador property
                    tributo.RazaoSocialEmpresa = nota.RazaoSocialPrestador;
                    // set the TributoNome property to "ISS"
                    tributo.TributoNome = "ISS";
                    // set the TributoValor property to 5% of the Valor property
                    tributo.TributoValor = (nota.Valor * 0.05m).ToString();
                    // add the tributo to the list
                    tributos.Add(tributo);
                }
                
                // add a new record in the tributos table for each tributo in the list
                foreach (var tributo in tributos) {
                    // create a new mysql command object
                    MySqlCommand command = connection.CreateCommand();
                    // set the command text
                    command.CommandText = "INSERT INTO tributos " +
                        "(empresa_id, razao_social_empresa, tributo_nome, tributo_valor) " +
                        "VALUES (@empresa_id,@razao_social_empresa,@tributo_nome,@tributo_valor)";
                    // set the parameters from the response object
                    command.Parameters.AddWithValue("@empresa_id", tributo.EmpresaId);
                    command.Parameters.AddWithValue("@razao_social_empresa", tributo.RazaoSocialEmpresa);
                    command.Parameters.AddWithValue("@tributo_nome", tributo.TributoNome);
                    command.Parameters.AddWithValue("@tributo_valor", tributo.TributoValor);
                    // try to execute the command
                    try {
                        command.ExecuteNonQuery();
                        // if it succeeds, log the command
                        _logger.LogInformation("Insert into tributos database succeeded");
                    } catch (Exception e) {
                        // if it fails, log the error
                        _logger.LogError(e.ToString());
                    }
                }
                // close the connection
                connection.Close();
            }
            // create a new HTTP response object
            HttpResponseData response = req.CreateResponse(HttpStatusCode.OK);
            // set the body of the response to the parsed response
            response.WriteString(responseMessage.ToString());
            // return the response
            return response;
        }
    }
}

