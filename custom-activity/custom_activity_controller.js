const config = require('../config');
const mc = require('../modules/mc');
const zalo = require('../modules/zalo');
const messenger = require('../modules/messenger');
const viber = require('../modules/viber');
const logger = require('../modules/logger');
const axios = require('axios');

const controller = {}; 

controller.getOAs = () => {
    let dataExtensionName = config.MC.viberDEName;
    let fields = ['Viber_OA_Id','Viber_OA_Name', 'Active'];
    let filter = {				
        	leftOperand: 'Active',
        	operator: 'equals',
        	rightOperand: true
   	}

    return mc.getDERows(dataExtensionName, fields, filter)
  
};

controller.getDataExtensions = () => {
    return mc.getAllDataExtensions();
};

controller.getDataExtensionFields = (dataExtensionKey) => {
    return mc.getDataExtensionFields(dataExtensionKey);
};

controller.getCustomContentBlocks = () => {
    let fields = ['Id', 'Name'], 
        contentFolderName = config.MC_ContentCategories.CustomBlockPrefix, 
        assetTypeId = config.MC_AssetTypes.CustomBlock;

    let query = { 
        "leftOperand":
        {
            "property":"name",
            "simpleOperator":"startsWith",
            "value": contentFolderName
        },
        "logicalOperator":"AND",
        "rightOperand":
        {
            "property":"assetType.id",
            "simpleOperator":"equals",
            "value": assetTypeId
        }
    }

    return mc.getContent(fields, query);
}

controller.getContentById = (contentId) => {
    let fields = ['content'];
    return mc.getContentById(contentId, fields)
};

controller.execute = async (req, res) => {
    console.log('Activity controller.execute is called - req.body:', req.body);
    console.log('Activity controller.execute is called - req', req);
    
    let obj = req.body.inArguments[0];

    let oa = obj.oa,
        oaName = obj.oaName, 
        contactKey = obj.contactKey, 
        contentId = obj.contentId, 
        dataExtensionName = obj.dataExtensionName, 
        dataExtensionKey = obj.dataExtensionKey, 
        subscriberKeyField = obj.subscriberKeyField, 
        zaloIdField = obj.zaloIdField, //ZNS this field will be the phone field
        recipientId = obj.recipient_id,
        targetFields = obj.targetFields,
        journeyId = req.body.journeyId,
        activityId = req.body.activityId,
        activityInstanceId = req.body.activityInstanceId,
        messageType = obj.messageType,
        messageName = obj.messageName;
        //messageTagType = obj.messageTagType;

    // console.log('contactKey:', contactKey);
    // console.log('oa:', oa); 
    // console.log('contentId:', contentId);
    // console.log('dataExtensionKey:', dataExtensionKey);
    // console.log('dataExtensionName:', dataExtensionName);
    // console.log('subscriberKeyField:', subscriberKeyField);
    // console.log('zaloIdField:', zaloIdField);
    // console.log('targetField:', targetFields);
    // console.log('journeyId:', journeyId);
    // console.log('actvityId:', activityId);
    // console.log('actvityInstanceId:', activityInstanceId);
    // console.log('messageType:', messageType);
    // console.log('messageName:', messageName);
    
    const filter = {
        'leftOperand' : subscriberKeyField, 
        'operator' : 'equals',
        'rightOperand' : contactKey
    };

    try {
        const targetRecords = await mc.getDERows(dataExtensionName, targetFields, filter); 

        if(targetRecords && targetRecords.length > 0){
            const record = targetRecords[0]; 
            const fields = 'meta';
            const recipient_id = record[recipientId];

            let oaRecords = await mc.getDERows(config.MC.viberDEName, ['token', 'Viber_OA_Name', 'Avatar'], {
                'leftOperand' : 'Viber_OA_Id', 
                'operator' : 'equals',
                'rightOperand' : oa
            });
            const token = oaRecords[0].token; 
            oaName = oaRecords[0].Viber_OA_Name;
            const avatar =  oaRecords[0].Avatar;
            const content = await mc.getContentById(contentId, fields); 
            let contentMessage = content.meta.options.customBlockData.message;

            //Only personalize the content for 3 types of messages
            if(content.meta.options.customBlockData.type === 'text-area' || content.meta.options.customBlockData.type === 'form-photo'
            || content.meta.options.customBlockData.type === 'list-button'){
                for(const prop in record){
                    let expr = `%%${prop}%%`;
                    const replacer = new RegExp(expr, 'gi')
                    contentMessage.text = contentMessage.text.replace(replacer, record[prop]);
                }
            }
            
            //File message
            if(content.meta.options.customBlockData.type === 'notice') {
                let fileName = content.meta.options.customBlockData.fileName,
                fileType = content.meta.options.customBlockData.fileType,
                fileId = content.meta.options.customBlockData.fileId,
                fileUrl = content.meta.options.customBlockData.u['notice-file'];

                //Download file from MC 
                let fileResponse = await axios({
                    method : 'get',
                    url : fileUrl,
                    responseType : 'Stream',
                });

                const zaloFileResponse = await zalo.uploadFile(token, fileResponse.data, fileName);
                const zaloFileToken = zaloFileResponse.data.data.token;
                contentMessage.attachment.payload.token = zaloFileToken;

            }

            let messengerPayload = {
                "receiver":recipient_id,
                "min_api_version":1,
                "sender":{
                   "name": oaName,
                   "avatar":avatar
                },
                "tracking_data":"tracking data",
                "type":"text",
                "text": ("text" in contentMessage ? contentMessage.text : contentMessage)
             };
             logger.info('[Viber Request]' + '= ' +  JSON.stringify(messengerPayload));
            let messengerResponse = undefined;
            try {
                const messengerResponseFull = await viber.sendMessage(token, messengerPayload);
                messengerResponse = messengerResponseFull.data;
            } catch(err) {
                if (err.response.data) {
                    messengerResponse = err.response.data;
                    console.log("err->", err.response.data);
                }
            } 
            const mcRecord = { 
                'Viber_Id': oa, 
                "Receiver_Id": recipient_id,
                'Message_Name': messageName,
                'Message_Content': ("text" in contentMessage ? contentMessage.text : contentMessage),
                'Campaign_Journey_Name' : journeyId,
                'Journey_Activity_ID': activityInstanceId,
                'Message_Type': messageType
            };
            logger.info('[Viber Response]' + '= ' +  JSON.stringify(messengerResponse));
            if(messengerResponse != undefined && "message_token" in messengerResponse) {
                mcRecord['Message_ID'] = messengerResponse.message_token;
                mcRecord['Sent_Date'] = new Date(Date.now()).toISOString();
            }
            else if (messengerResponse != undefined ){ //Messenger return error
                logger.error('[Facebook Response Error]', messengerResponse);
                mcRecord['API_Response_Error'] = messengerResponse.error.message;
                mcRecord['Message_ID'] = `ERROR(${messengerResponse.error.code})_${Date.now()}`;
            }
        
            //Tracking Sent Messages
            mc.createDERow(config.MC.viberSendLogDE, mcRecord)
            .then(mcResponse => {
                logger.info('MC Response:', mcResponse);
            }).catch(mcError =>{
                logger.error('MC Error:', mcError);
            });
            res.status(200).send({
                status: 'ok',
            });
        }
    }
    catch(err) {
        logger.error('[Exception in custom_activity_controller.execute]', err);
        res.sendStatus(500);
    }
};

module.exports = controller; 
