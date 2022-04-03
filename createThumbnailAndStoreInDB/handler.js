const AWS = require("aws-sdk")
const sharp = require("sharp")
const uuid = require("uuid")

const DEFAULT_MAX_WIDTH = 200
const DEFAULT_MAX_HEIGHT = 200
const DDB_TABLE = "metadata"

const s3 = new AWS.S3()
const dynamodb = new AWS.DynamoDB()

let srcBucket = null
let srcKey = null
let dstBucket = null
let dstKey = null
let imageType = null

exports.hello = async (event) => {
    srcBucket = event.Records[0].s3.bucket.name
    srcKey = event.Records[0].s3.object.key
    dstBucket = srcBucket
    dstKey = "thumbs/" + srcKey

    imageType = getImageType(srcKey)
    const originalImage = await downloadImage()
    const resizedImage = await transformImage(originalImage)
    let response = await uploadThumbnaii(resizedImage)
    response = await storeMetadata(resizedImage.Metadata)
}

function getImageType(key) {
    const typeMatch = key.match(/\.([^.]*)$/)
    if (!typeMatch) {
        console.error(`Could not determine the image type for key: ${key}`)
        return
    }
    const imageType = typeMatch[1]
    if (imageType != "jpg" && imageType != "png") {
        console.error(`Unsupported image type: ${imageType}`)
        return
    }
    return imageType
}

async function downloadImage() {
    const image = await s3
        .getObject({
            Bucket: srcBucket,
            Key: srcKey,
        })
        .promise()
    return image
}

async function transformImage(image) {
    const resizedImage = {
        buffer: await sharp(image.Body)
            .resize({ width: DEFAULT_MAX_WIDTH })
            .toBuffer(),
        ContentType: image.ContentType,
        Metadata: image.metadata || {},
    }
    return resizedImage
}

async function uploadThumbnaii(image) {
    return await s3
        .putObject({
            Bucket: dstBucket,
            Key: dstKey,
            Body: image.buffer,
            ContentType: image.ContentType,
            Metadata: image.metadata,
        })
        .promise()
}

async function storeMetadata(metadata) {
    const params = {
        TableName: DDB_TABLE,
        Item: {
            id: { S: uuid.v1() },
            name: { S: srcKey },
            thumbnail: { S: dstKey },
            timestamp: { S: new Date().toJSON().toString() },
        },
    }

    if ("author" in metadata) {
        params.Item.author = { S: metadata.author }
    }
    if ("title" in metadata) {
        params.Item.title = { S: metadata.title }
    }
    if ("description" in metadata) {
        params.Item.description = { S: metadata.description }
    }
    return await dynamodb.putItem(params).promise()
}
