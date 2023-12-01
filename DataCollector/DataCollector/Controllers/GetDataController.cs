using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NIGetData.models;
using NIGetData.services;

namespace NIGetData.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class GetDataController : ControllerBase
    {
        private readonly GetDataService _getDataService;

        public GetDataController(GetDataService getDataService)
        {
            _getDataService = getDataService;
        }

        [HttpPost("get-data")]
        public IActionResult GetData([FromBody] BodyDataModel bodyDataModel)
        {
            var finalResult = _getDataService.GetData(bodyDataModel);

            return Ok(finalResult);
        }
    }
}
