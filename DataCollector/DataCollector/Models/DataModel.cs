using System.Security.Policy;
using System.Security.Principal;

namespace NIGetData.models
{
    public class DataModel
    {
        public DateTime Time { get; set; }
        public string Ne { get; set; }
        public float RSL_INPUT_POWER { get; set; }
        public float MaxRxLevel { get; set; }

        public float RSL_Deviation { get; set; }
    }
}

