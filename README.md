# Avro Mocker
Generate mock data based on an Apache Avro schema and specific cardinality settings.

## Usage
The program is intended to be used as a command-line application. To run it, invoke:

```shell
java -jar avro-mocker.jar \
  -schema my-schema.avsc \
  -result my-data.avro
```

The program will ask a number of questions regarding how many rows to generate as well as the structure of the columns. For most of the questions, a blank input is sufficient to use the default settings.

### Example Settings
**Incrementing by 1:**

```shell
Strategy for int 'id': incr
```

**Incrementing by 2:**

```shell
Strategy for int 'profile_id': incr scale 1
```

**Random value between 0 (inclusive) and 100 (exclusive):**

```shell
Strategy for int 'lucky_number': to 100
```

**Random value between 100 (inclusive) and 200 (exclusive):**

```shell
Strategy for int 'lucky_number': from 100 to 200
```

**Random even value between 0 (inclusive) and 100 (exclusive):**

```shell
Strategy for int 'lucky_even_number': to 50 scale 2
```

**Random date of 2017**

```shell
Strategy for int 'birthday': date from 20170101 to 20180101
```

**Random prime < 20**

```shell
Strategy for int 'my_prime': in 2, 3, 5, 7, 11, 13, 17, 19
```

**Repeating sequence of primes < 20**

```shell
Strategy for int 'my_prime': incr in 2, 3, 5, 7, 11, 13, 17, 19
```

**Random enum constant**

```shell
Strategy for enum 'gender': 
```

**Random enum constant in selected**

```shell
Strategy for enum 'gender': in MALE,OTHER
```

## License
Copyright 2017 Speedment, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.