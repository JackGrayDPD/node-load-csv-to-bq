require('dotenv').config();
const yargs = require('yargs');
// Import the Google Cloud client libraries
const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');

// Instantiate clients
const bigquery = new BigQuery();
const storage = new Storage();

const args = yargs(process.argv.slice(2)).parse();
const {datasetId, tableId, bucketName, fileName} = args;

// Imports a GCS file into a table and appends to existing table if table already exists.
async function loadCSVFromGCSAppend() {
  if (!datasetId || !tableId || !bucketName || !fileName) {
    throw 'One or more arguments are missing from the command: must include --datasetId, --tableId, --bucketName, --fileName.'
  }

  // Configure the load job. For full list of options, see:
  // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
  const metadata = {
    sourceFormat: 'CSV',
    skipLeadingRows: 0,
    schema: {
      // bq show --format=prettyjson PROJECT_NAME:DATASET_NAME.TABLE_NAME > schema.json
      fields: [
        {
          mode: "REQUIRED",
          name: "ParcelKey",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "ParcelNumber",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "ParcelDateSuffix",
          type: "INTEGER"
        },
        {
          mode: "NULLABLE",
          name: "LoadDate",
          type: "DATE"
        },
        {
          mode: "NULLABLE",
          name: "LastUpdate",
          type: "DATETIME"
        },
        {
          mode: "REQUIRED",
          name: "ParcelBrand",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NoOfParcels",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "ConsignmentNumber",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "ParcelOfCon",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ParcelOfStop",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ParcelOfStopOfAccount",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ParcelOfStopOfBrand",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ParcelOfConOfStop",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ParcelOfConOfStopOfAccount",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "NoOfParcelsDelivered",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "NoOfParcelsWeighed",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "NoOfSingleParcelCons",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "UndeliveredParcels",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "UndeliveredCons",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "MinChargeCons",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "MinChargeParcels",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "SortScanPresent",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "TrailerScanPresent",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RelabelPresent",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupScanPresent",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConfirmedWeight",
          type: "NUMERIC"
        },
        {
          mode: "NULLABLE",
          name: "FirstDeliveryTime",
          type: "TIME"
        },
        {
          mode: "NULLABLE",
          name: "ActualPODTime",
          type: "TIME"
        },
        {
          mode: "REQUIRED",
          name: "TravelledInToteBoxCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "TravelledInToteBoxDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveredRouteNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NoOfPODEvents",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "NoOfBarcodeScanEvents",
          type: "INTEGER"
        },
        {
          mode: "NULLABLE",
          name: "HubDayActualDate",
          type: "DATE"
        },
        {
          mode: "NULLABLE",
          name: "SortScanTime",
          type: "TIME"
        },
        {
          mode: "REQUIRED",
          name: "SortationHubCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ParcelRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "InsuranceRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "MiscellaneousRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "FuelSurchargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "SMSRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "EmailRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "BasicRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ExcessKiloRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "MinimumChargeTopUpRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ContractualLiabilityRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ExtendedLiabilityRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ThirdPartySurchargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ThirdPartySurchargeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ThirdPartySurchargeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FourthPartySurchargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "FourthPartySurchargeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FourthPartySurchargeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FuelSurchargeDetailRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "FuelSurchargeDetailCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FuelSurchargeDetailDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ScottishDeliveryZoneRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "TransitCongestionChargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "HandlingChargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "FailedCollectionRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "DutyOrVatChargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "CustomsExportCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustomsExportDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustomsExportRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "SHPNotificationsRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "OFDNotificationsRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "YCLNotificationsRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "PODNotificationsRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ACKNotificationsRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "RTCRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ALTAddressRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "AbsoluteRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "CostedRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "DepotCollectionCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "DepotDeliveryCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "LinehaulToHubCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "LinehaulToHubAdditionalCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "LinehaulFromHubCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "HubSortCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "HubGatewayCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "LinehaulGatewayCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "LinehaulFeederCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryWarehouseCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "CollectionWarehouseCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "OffshoreCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "InternationalClearanceCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "CentralOverheadCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "StationaryOverheadCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "EquipmentOverheadCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "AutomationOverheadCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "SupportOverheadCost",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "TotalCosts",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "NoOfParcelsCosted",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "NoOfConsCosted",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "PricingBasisCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PricingBasisDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionCommission",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "LabellingCommission",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "PartySurchargeCommission",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryCommission",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "OtherCommission",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "CurrencyCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CurrencyRate",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "RevenueAndCostsAvailable",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ProductCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ProductDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ProductGroupCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ProductGroupDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ServiceCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ServiceDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ServiceGeoCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ServiceGroupCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ServiceGroupDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ParcelTypeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ParcelTypeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SortationGroupCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SortationGroupDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "InboundOutboundCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "InboundOutboundDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustParcelOutcomeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustParcelOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustParcelOutcomeGroupCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustParcelOutcomeGroupDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustParcelFailureCauseCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustParcelFailureCauseDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustConOutcomeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustConOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustConOutcomeGroupCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustConOutcomeGroupDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustConFailureCauseCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustConFailureCauseDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ParcelStatusCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ParcelStatusDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionPointOccCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionPointOccDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionTypeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionTypeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LabelMethodCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LabelMethodDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ParcelLabelType",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionPostCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionSector",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionOuterSec",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionAreaCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryPostCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliverySector",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryOuterSec",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryAreaCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "AccountKey",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "AccountNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AccountName",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "CustomerNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustomerOrAccountNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CustomerName",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionCountryCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionCountryDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionWorldRegionCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionWorldRegionDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "IntlCollectionDepotCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionZone",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryCountryCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryCountryDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryWorldRegionCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryWorldRegionDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "IntlDeliveryDepotCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryZone",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AnalysisZoneCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AnalysisZoneDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AlignmentDepotNumberInt",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "AlignmentDepotNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AlignmentDepotNetwork",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "AlignmentDepotName",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AlignmentDepotRegionNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AlignmentDepotRegion",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionDepotNumberInt",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "CollectionDepotNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionDepotNetwork",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionDepotName",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionDepotRegionNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionDepotRegion",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryDepotNumberInt",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryDepotNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryDepotNetwork",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryDepotName",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryDepotRegionNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryDepotRegion",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "CollectionActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "CollectionYear",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionQuarter",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionPeriod",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "CollectionWeek",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "IntendedDeliveryActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "IntendedDeliveryYear",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "IntendedDeliveryQuarter",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "IntendedDeliveryPeriod",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "IntendedDeliveryWeek",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "ActualCompletionActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "ActualCompletionYear",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ActualCompletionQuarter",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ActualCompletionPeriod",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ActualCompletionWeek",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "AdjustedDeliveryActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "AdjustedDeliveryYear",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AdjustedDeliveryQuarter",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AdjustedDeliveryPeriod",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AdjustedDeliveryWeek",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ContributionBandCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ContributionBandDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "WeightBandCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "WeightBandDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryTimeBandCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryTimeBandDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryImagePresentCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryImagePresentDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "HubscanTypeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "HubscanTypeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConfirmInDepotCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConfirmInDepotDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryPointOccCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryPointOccDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryMethodCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryMethodDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryResultCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryResultDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SignatureCaptureCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SignatureCaptureDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NoTraceReasonCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NoTraceReasonDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LeaveOnAuthorityCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LeaveOnAuthorityDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ManagedDeliveryOptionCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LabellerType",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryChecklist",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "OFDPresentCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "OFDPresentDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryETACode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryETADesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryNotificationCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NotificationPresentCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NotificationPresentDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NotificationTypeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NotificationTypeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NotificationMethodCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "NotificationMethodDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ResponsePresentCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ResponsePresentDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ResponseTypeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ResponseTypeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ResponseMethodCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ResponseMethodDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RedeliveryResponseCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ServiceAdjustmentCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ServiceAdjustmentDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotParcelOutcomeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotParcelOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotParcelOutcomeGroupCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotParcelOutcomeGroupDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotParcelFailureCauseCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotParcelFailureCauseDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotConOutcomeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotConOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotConOutcomeGroupCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotConOutcomeGroupDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotConFailureCauseCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DepotConFailureCauseDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RedeliveryUpgradeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RedeliveryUpgradeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DataBeforeOFD",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ImageRequested",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DeliveryAttempts",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "HubScanWindowCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "HubScanWindowDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DirectToHubCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DirectToHubDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FirstHubScanTypeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FirstHubScanTypeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LocalTrafficCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LocalTrafficDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion1Code",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion1Desc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion1Answer",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion2Code",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion2Desc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion2Answer",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion3Code",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion3Desc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion3Answer",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion4Code",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion4Desc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion4Answer",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion5Code",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion5Desc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion5Answer",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion6Code",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion6Desc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ChecklistQuestion6Answer",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "FirstInvoiceActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "FirstInvoiceYear",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FirstInvoiceQuarter",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FirstInvoicePeriod",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FirstInvoiceWeek",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "LastInvoiceActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "LastInvoiceYear",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LastInvoiceQuarter",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LastInvoicePeriod",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "LastInvoiceWeek",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "AccrualActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "AccrualYear",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AccrualQuarter",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AccrualPeriod",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AccrualWeek",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClearingTypeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClearingTypeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClearingBasisCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClearingBasisDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClearingStatusCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClearingStatusDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClearingAmount",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "DataPresentAtExportCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DataPresentAtExportDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DataPresentCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DataPresentDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "NoOfTransitDays",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "TransitDaysRangeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "TransitDaysRangeDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "NoOfTransitDaysDiff",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "TransitDaysDiffRangeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "TransitDaysDiffRangeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "HubAvoidanceCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "HubAvoidanceDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DataShipperCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DataShipperDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RfiActionCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RfiActionDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RfiResponseCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RfiResponseDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupCollectionShopCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "PickupCollectionActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "PickupCollectionOutcomeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupCollectionOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupDeliveryShopCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "PickupDeliveryActualDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "PickupDeliveryOutcomeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupDeliveryOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupDeliveryReasonCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupDeliveryReasonDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "PickupDaysInShop",
          type: "INTEGER"
        },
        {
          mode: "NULLABLE",
          name: "PickupTime",
          type: "TIME"
        },
        {
          mode: "REQUIRED",
          name: "PickupTimeBandCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupTimeBandDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupDayOfWeekCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupDaysInShopCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PickupDaysInShopDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ActualPODTimeWindowCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ActualPODTimeWindowDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "DropOffTime",
          type: "TIME"
        },
        {
          mode: "REQUIRED",
          name: "DropOffTimeBandCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DropOffTimeBandDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SMSDataQualityCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SMSDataQualityDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "EmailDataQualityCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "EmailDataQualityDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConsumerChoiceCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConsumerChoiceDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConsumerRatingCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConsumerRatingDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConsumerCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "DeliveryUDPRN",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "ConsumerReturnDepotCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ConsumerReturnDriver",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PreciseOutcomeCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PreciseOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SendersConsumerCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AppVoucherAmount",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "AppPaymentMethod",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SellersCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "SellersName",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "GroupCustomerName",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "OriginCountryCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "OriginCountryDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "OriginDepot",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "PerishableDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "PriorityAccountCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "PriorityPropertyCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClickAndCollect",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "DataProcessedDate",
          type: "DATE"
        },
        {
          mode: "NULLABLE",
          name: "DataProcessedTime",
          type: "TIME"
        },
        {
          mode: "NULLABLE",
          name: "CollectionTime",
          type: "TIME"
        },
        {
          mode: "NULLABLE",
          name: "ShopTargetDate",
          type: "DATE"
        },
        {
          mode: "NULLABLE",
          name: "PreciseTargetDate",
          type: "DATE"
        },
        {
          mode: "REQUIRED",
          name: "CustomerRef1",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AEOComplianceCode",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "AEOComplianceDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "CollectionUKActualDate",
          type: "DATE"
        },
        {
          mode: "NULLABLE",
          name: "VolumiserHub",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "VolumiserType",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "VolumiserParcelType",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "VolumiserLength",
          type: "INTEGER"
        },
        {
          mode: "REQUIRED",
          name: "VolumiserGirth",
          type: "INTEGER"
        },
        {
          mode: "NULLABLE",
          name: "ClaimOutcomeCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "ClaimOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ClaimOutcomeValue",
          type: "NUMERIC"
        },
        {
          mode: "NULLABLE",
          name: "PodOutcome",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "PodOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "ParcelTypeExclVolumiser",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "FirstHubScanUserName",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "PartnerWeight",
          type: "NUMERIC"
        },
        {
          mode: "NULLABLE",
          name: "DeclaredWeight",
          type: "NUMERIC"
        },
        {
          mode: "NULLABLE",
          name: "ActualWeight",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "Weighed",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "DutiesAndTaxes",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "FailedExportFee",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "ExportReturnFee",
          type: "NUMERIC"
        },
        {
          mode: "NULLABLE",
          name: "VolumisingRequired",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "OEXFee",
          type: "NUMERIC"
        },
        {
          mode: "NULLABLE",
          name: "ConsumerLikedCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "ConsumerParcelComplimentCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "ConsumerDriverComplimentCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "OriginalProductCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "OriginalProductDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "OriginalProductGroupCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "OriginalProductGroupDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "BillingServiceCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "BillingServiceDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "BillingServiceGroupCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "BillingServiceGroupDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "NotAtHomeSamedayOutcomeCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "NotAtHomeSamedayOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "NotAtHomeSamedayDriver",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "NotAtHomeSamedayShop",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "NotAtHomeSamedayDate",
          type: "DATE"
        },
        {
          mode: "NULLABLE",
          name: "PeakDowngradeCode",
          type: "INTEGER"
        },
        {
          mode: "NULLABLE",
          name: "RelovedOutcomeCode",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "RelovedOutcomeDesc",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "RelovedOriginAccountNumber",
          type: "STRING"
        },
        {
          mode: "REQUIRED",
          name: "Class1HGVChargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "NonCompatibleChargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "PeakChargeRevenue",
          type: "NUMERIC"
        },
        {
          mode: "REQUIRED",
          name: "HubBoxIntegration",
          type: "STRING"
        },
        {
          mode: "NULLABLE",
          name: "FirstCADDate",
          type: "DATE"
        },
        {
          mode: "NULLABLE",
          name: "FirstCADTime",
          type: "TIME"
        }
      ],
    },
    // Set the write disposition to append to existing table.
    writeDisposition: 'WRITE_APPEND',
    location: 'europe-west2',
  };

  // Load data from a Google Cloud Storage file into the table
  const dataset = bigquery.dataset(datasetId);
  const table = dataset.table(tableId);

  const [job] = await table.load(storage.bucket(bucketName).file(fileName), metadata);
  // load() waits for the job to finish
  console.log(`Job ${job.id} completed.`);

  // Check the job's status for errors
  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }
}
loadCSVFromGCSAppend();