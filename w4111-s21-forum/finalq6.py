from pymongo import MongoClient
import pandas as pd
client = MongoClient("mongodb://localhost:27017/admin?readPreference=primary&appname=MongoDB%20Compass&ssl=false")

orders = client['classic_models']['orders']

aggregator = [
    {
        '$addFields': {
            'shippedYear': {
                '$year': [
                    '$shippedDate'
                ]
            },
            'shippedMonth': {
                '$month': [
                    '$shippedDate'
                ]
            },
            'shippedQuarter': {
                '$ceil': {
                    '$divide': [
                        {
                            '$month': [
                                '$shippedDate'
                            ]
                        }, 3
                    ]
                }
            }
        }
    }, {
        '$unwind': {
            'path': '$orderDetails'
        }
    }, {
        '$project': {
            'orderNumber': 1,
            'customerNumber': 1,
            'status': 1,
            'shippedYear': 1,
            'shippedMonth': 1,
            'shippedQuarter': 1,
            'quantityOrdered': '$orderDetails.quantityOrdered',
            'priceEach': '$orderDetails.priceEach',
            'totalSale': {
                '$multiply': [
                    '$orderDetails.quantityOrdered', '$orderDetails.priceEach'
                ]
            },
            'productCode': '$orderDetails.productCode'
        }
    }, {
        '$lookup': {
            'from': 'customers',
            'localField': 'customerNumber',
            'foreignField': 'customerNumber',
            'as': 'customerInfo'
        }
    }, {
        '$lookup': {
            'from': 'products',
            'localField': 'productCode',
            'foreignField': 'productCode',
            'as': 'productInfo'
        }
    }, {
        '$project': {
            'orderNumber': 1,
            'customerNumber': 1,
            'status': 1,
            'shippedYear': 1,
            'shippedMonth': 1,
            'shippedQuarter': 1,
            'quantityOrdered': 1,
            'priceEach': 1,
            'totalSale': 1,
            'productCode': '$productCode',
            'customerInfo': {
                '$arrayElemAt': [
                    '$customerInfo', 0
                ]
            },
            'productInfo': {
                '$arrayElemAt': [
                    '$productInfo', 0
                ]
            }
        }
    }, {
        '$project': {
            'orderNumber': 1,
            'customerNumber': 1,
            'status': 1,
            'shippedYear': 1,
            'shippedMonth': 1,
            'shippedQuarter': 1,
            'quantityOrdered': 1,
            'priceEach': 1,
            'region': '$customerInfo.region',
            'totalSale': 1,
            'country': '$customerInfo.country',
            'state': '$customerInfo.state',
            'city': '$customerInfo.city',
            'productCode': '$productCode',
            'productScale': '$productInfo.productScale',
            'productLine': '$productInfo.productLine',
            'productVendor': '$productInfo.productVendor'
        }
    }
]
df = pd.DataFrame(orders.aggregate(aggregator)).drop(columns=["_id"])
wide_flat_dataframe = df[df['status'] == 'Shipped'].round({'shippedYear': 0, 'shippedMonth': 0, 'shippedQuarter': 0})

print(wide_flat_dataframe)