# Changelog

## [0.9.1](https://github.com/xyngular/py-xdynamo/compare/v0.9.0...v0.9.1) (2024-10-28)


### Bug Fixes

* between fixes created bugs with is_in operator, fixed that as well. ([28d7f53](https://github.com/xyngular/py-xdynamo/commit/28d7f53a1b95ca6693daf73eeeb8221e18368eac))
* between operator was broken, fixed it up. ([f7d7318](https://github.com/xyngular/py-xdynamo/commit/f7d7318adc08e79f89ce99f44765ae2769a4b755))
* only separate the results if range/between used. ([4347125](https://github.com/xyngular/py-xdynamo/commit/43471256d50d5e6e9d1aa5a492f33ffe442966a3))

## [0.9.0](https://github.com/xyngular/py-xdynamo/compare/v0.8.2...v0.9.0) (2024-10-04)


### Features

* add consistent read feature. ([9c687f6](https://github.com/xyngular/py-xdynamo/commit/9c687f672b10cd77832c6b1626a5646a6fa07f71))

## [0.8.2](https://github.com/xyngular/py-xdynamo/compare/v0.8.1...v0.8.2) (2024-10-02)


### Bug Fixes

* error message string had incorrect information. ([b4c6628](https://github.com/xyngular/py-xdynamo/commit/b4c6628bbab45301c7b83af68227f0abbee49be3))

## [0.8.1](https://github.com/xyngular/py-xdynamo/compare/v0.8.0...v0.8.1) (2024-08-06)


### Bug Fixes

* allow class structural issue exception to propagate up. ([eb3c174](https://github.com/xyngular/py-xdynamo/commit/eb3c174bdaeef92388a18b2f8d83e8831ca217ad))
* type-hint UUID for `get_via_id` (it always worked, but now type-hint is correct). ([44979d1](https://github.com/xyngular/py-xdynamo/commit/44979d1deff191a76ba0d3bba71a12d16e7dd1a1))

## [0.8.0](https://github.com/xyngular/py-xdynamo/compare/v0.7.0...v0.8.0) (2023-12-20)


### Features

* support python 3.12 ([88c4b34](https://github.com/xyngular/py-xdynamo/commit/88c4b34b01d3b9c4dcf5f8840426c104fcc9daa5))

## [0.7.0](https://github.com/xyngular/py-xdynamo/compare/v0.6.0...v0.7.0) (2023-11-17)


### Features

* Update README.md ([231940b](https://github.com/xyngular/py-xdynamo/commit/231940bfe7aa7df0cdc51f916d3c986ff1eabd12))

## [0.6.0](https://github.com/xyngular/py-xdynamo/compare/v0.5.0...v0.6.0) (2023-11-17)


### Features

* publish updated readme. ([21c1fc9](https://github.com/xyngular/py-xdynamo/commit/21c1fc9f87e31a41bd94b4c89442e237de80928d))

## [0.5.0](https://github.com/xyngular/py-xdynamo/compare/v0.4.0...v0.5.0) (2023-09-27)


### Features

* conditional updates/deletes + updating if removal + table scanning support ([38c1e8c](https://github.com/xyngular/py-xdynamo/commit/38c1e8c27443e163d214788443db1fb9ee21017e))
* update dependencies to get latest xmodel. ([34b24f5](https://github.com/xyngular/py-xdynamo/commit/34b24f5ad5e5104636b510e98596db7daaa38ad5))

## [0.4.0](https://github.com/xyngular/py-xdynamo/compare/v0.3.0...v0.4.0) (2023-07-07)


### Features

* can use any future version of xmodel up until &lt;1.0.0. ([2cd1508](https://github.com/xyngular/py-xdynamo/commit/2cd150898568a1573086972974a014d64c212228))

## [0.3.0](https://github.com/xyngular/py-xdynamo/compare/v0.2.0...v0.3.0) (2023-05-02)


### Features

* add Self + dataclass transform features from Python 3.11 (but still works with &lt; 3.11) ([c7d3b68](https://github.com/xyngular/py-xdynamo/commit/c7d3b68e9044e4acef97687ebd5bdf69bdece658))

## [0.2.0](https://github.com/xyngular/py-xdynamo/compare/v0.1.1...v0.2.0) (2023-05-01)


### Features

* finish rest of xmodel-dynamo to just xdynamo rename. ([6de99ab](https://github.com/xyngular/py-xdynamo/commit/6de99ab5a1b3821d1f91381e6b71023bcdced9eb))
* make `xcon` optional dependency. ([85cd8e4](https://github.com/xyngular/py-xdynamo/commit/85cd8e445206a4ae6fc2a31096d9a9cdd6ec2030))
* only auto-create tables if in `unittest` or `local` environments. ([e42a0f6](https://github.com/xyngular/py-xdynamo/commit/e42a0f65e960081dce0aea8d78d6b3248c62be09))
* rename/simplify library name to `xdynamo` (file chages). ([f3d6590](https://github.com/xyngular/py-xdynamo/commit/f3d659034cd1ade3965b9283c28480dd0c87e1cd))
* rename/simplify library to `xdynamo`. ([247f5fc](https://github.com/xyngular/py-xdynamo/commit/247f5fc6e03249fb3e32f0c5a0990567a8976731))
* upgrade moto + adapt to it. ([f898db6](https://github.com/xyngular/py-xdynamo/commit/f898db6e8a2dab44e929f84317bfd92c1cae7bad))

## [0.1.1](https://github.com/xyngular/py-xdynamo/compare/v0.1.0...v0.1.1) (2023-04-15)


### Bug Fixes

* doc links. ([6fccf08](https://github.com/xyngular/py-xdynamo/commit/6fccf0841b3855a3baae0c77469217cf03b55af3))
* license ([ee1e94e](https://github.com/xyngular/py-xdynamo/commit/ee1e94e9348405e41d5601300aafef3715d99a49))


### Documentation

* added doc links. ([3504efa](https://github.com/xyngular/py-xdynamo/commit/3504efaa97c92a035a30593cc1130d4fc376215f))

## 0.1.0 (2023-04-15)


### Features

* initial code import. ([f80ff68](https://github.com/xyngular/py-xdynamo/commit/f80ff68513529d53101a532b3bd2e1d956f611bc))


### Bug Fixes

* specify default region for unit tests. ([4c4b9d5](https://github.com/xyngular/py-xdynamo/commit/4c4b9d5441b89dd61b9e7eddc6bb2f81da1ba039))
* updated xmodel, it had a bug. ([bd3cff1](https://github.com/xyngular/py-xdynamo/commit/bd3cff18b8ae862e3f59668624c60c642dd14fa6))


### Documentation

* add initial basic docs. ([0774831](https://github.com/xyngular/py-xdynamo/commit/07748318cc79ead3020ccf09b1408792a1a30fb3))
