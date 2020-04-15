# hwget
Hwget is used to download data with Huawei Cloud.

## Requirement 
* python 2.7+ or python3.5+
* [huaweicloud-sdk-python](https://github.com/huaweicloud/huaweicloud-sdk-python)
* [huaweicloud-sdk-python-obs](https://github.com/huaweicloud/huaweicloud-sdk-python-obs)

## Usage
```python
from hwget import Hwget

ak = "your_ak"
sk = "your_sk"
region = "ap-southeast-1"  # Hong Kong
project_id = "my_project_id"
bucket = "bucket_you_want_to_save_to"

cloud = Hwget(
    ak=ak,
    sk=sk, 
    region=region, 
    project_id=project_id,
    bucket=bucket
)

cloud.get([
    "https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos1/sra-pub-run-5/SRR1609905/SRR1609905.2",
    "https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos2/sra-pub-run-7/SRR1609906/SRR1609906.2",
    "https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos1/sra-pub-run-5/SRR1609907/SRR1609907.2",
])

```
