# 1.0.0 (2023-05-31)


### Bug Fixes

* adding latest code to main branch ([0f6ee7a](https://dev.azure.com/predictintel/crawlers/_git/attachmentsmanager/commit/0f6ee7adcc5aafabe7ddd2b44189d434865c6d63))
* fixed merge conflicts ([842836f](https://dev.azure.com/predictintel/crawlers/_git/attachmentsmanager/commit/842836f66ca61eee6dd32034f29066cccd632a53))


### Features

* add tags for image analysis ([8ca2a09](https://dev.azure.com/predictintel/crawlers/_git/attachmentsmanager/commit/8ca2a0913e2a3955253cc9bf6cd8c225d3344609))
* new endpoint to crawl missing profile pic ([b5eed52](https://dev.azure.com/predictintel/crawlers/_git/attachmentsmanager/commit/b5eed526546edc9b738cc095fecad13e37f8451c))

# [0.4.0](https://gitlab.com/cybersmart/apis1/attachmentsmanager/compare/0.3.0...0.4.0) (2023-04-30)


### Bug Fixes

* doing .get for possibly missing metadata keys ([6aa70ff](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/6aa70ff75c98f2e53ea731993edf65c04b87ff14))
* fileUrl value to full path ([da97032](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/da97032123cc97c79d58321292374baa060d3dd8))
* fix bug with the condition ([8e15f19](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/8e15f194feb87bd66c502ea5fd9b229fc5812092))
* path, fileUrl change to str instead of list ([da93244](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/da932448987e1b13c7c0d3492a59df4d8b6caff5))
* read metadata key while processing request ([9d943f7](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/9d943f76ea962ed07894cf1218903a01e87323b8))
* remove bucket from cleaned fileUrl to file collection ([25d17f5](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/25d17f5b61cd27872185d9cc4a067b2f0c02dad0))
* same val for path and fileUrl in file collection ([2f31cc9](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/2f31cc9e790b4ff7720999b59dc82c4f110348f1))
* use attachment service URL in file coll upload instead of redirect URL ([23a1ce2](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/23a1ce2a25820ceb3a20d02fab019419787aed25))


### Features

* add minio proxy ([8dca588](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/8dca58855164881df4d3076ae569f4e95f09eb64))
* add minio proxy mode for proxying images interally instead of redirecting to azure blob ([cffa040](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/cffa040fa4f94789012cb00e226b3269da4b4ec1))
* document tagging ([3384fe1](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/3384fe1918d7f048a27d57475530cf1ae03389bb))

# [0.3.0](https://gitlab.com/cybersmart/apis1/attachmentsmanager/compare/0.2.1...0.3.0) (2023-03-22)


### Features

* added 'cdninstagram' to priority 2 check ([2ea2a46](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/2ea2a4683692f35c3dc3c7404030f81a2f80422c))
* new endpoint 'upload_b64' for b64 image uploading ([46fbb44](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/46fbb44667b1a5e5b3fb484f70c70a72ff81cfb6))

## [0.2.1](https://gitlab.com/cybersmart/apis1/attachmentsmanager/compare/0.2.0...0.2.1) (2022-12-20)


### Bug Fixes

* add fix to dedupe same media upload from FB ([8efb239](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/8efb2397f538c5f57b8622431c440326baa131d8))

# [0.2.0](https://gitlab.com/cybersmart/apis1/attachmentsmanager/compare/0.1.0...0.2.0) (2022-12-13)


### Bug Fixes

* change response type for dl endpoint ([533ea86](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/533ea86d2823c991ed43cc11436d27a8992f6770))
* dslib protobuf dependency issue fix ([5169eb2](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/5169eb210b17e6b17c67bbe1c6d4a5f09ec64fc3))
* fix in response for dl endpoint ([bb162d5](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/bb162d563c255169e3ad6846b290827af1c2671b))
* fix in return type for new endpoint ([56d0867](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/56d086737334fe23d0a66c63cd938dca72e9f032))
* removed extra '/' char from dl url build ([49d2aa3](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/49d2aa374e1ac651afb8f74643e2bd74727e7ba1))
* response type update and dl url config for recover image ([7ac6b3f](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/7ac6b3f3668d18f40fa1f396f8953dcc6c91a136))
* update in logging ([8469d59](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/8469d59010cdc03894548a7a0cf06dea9c372e60))
* update return type for recover_image endpoint ([fc17f42](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/fc17f421bbbb02f7911f3df0cdadbe4d65425978))
* using content instead of header param for dl return ([e13b422](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/e13b42286807540ea3ea94135a532ba25560d71b))


### Features

* dummy commit 2 ([ed62b92](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/ed62b9268ad8602545a86b70ba955047166a9d55))
* new endpoint to recrawl missing profile pic ([6ab650d](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/6ab650d6369233e5ed340a11529d0ff7c0b511ea))
* new GET endpoint to recover image ([fa5bad3](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/fa5bad3073d365f2628a60a915023baf4a96e04a))
* reset status if recrawl retriggered ([baa03b5](https://gitlab.com/cybersmart/apis1/attachmentsmanager/commit/baa03b529657a0d57486d5c10a44ffb765c3570e))
