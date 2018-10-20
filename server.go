package main

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

type Opinion struct {
	Id        int     `json:"-"`
	Created   int64   `json:"created"`
	Longitude float32 `json:"longitude"`
	Latitude  float32 `json:"latitude"`
	Body      string  `json:"body"`
	Dist      float32 `json:"dist,omitempty"`
	TTL       int     `json:"ttl,omitempty"`
}

type Location struct {
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	Radius    int     `json:"radius"`
}

type Reaction struct {
	Id        int `json:"id"`
	Created   int `json:"created"`
	OpinionId int `json:"opinion_id"`
	Emotion   int `json:"emotion"`
}

type Emotion struct {
	Id   int    `json:"id"`
	Icon string `json:"icon"`
	Name string `json:"name"`
}

var redisConn redis.Conn

func main() {
	var err error
	redisConn, err = redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer redisConn.Close()

	http.HandleFunc("/add/opinion", addOpinionHandler)
	http.HandleFunc("/opinions/near", getOpinionsNearMeHandler)

	err = http.ListenAndServe(":80", nil)
	if err != nil {
		panic(err)
	}
}

func addOpinionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.NotFound(w, r)
	} else {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}

		var opinion Opinion
		err = json.Unmarshal(data, &opinion)
		if err != nil {
			panic(err)
		}
		if opinion.Longitude == 0 || opinion.Latitude == 0 || opinion.Body == "" {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			response, status := addOpinion(opinion)
			w.WriteHeader(status)
			w.Write([]byte(response))
		}
	}
}

func addOpinion(opinion Opinion) (string, int) {
	opinionId, err := redis.Int(redisConn.Do("INCR", "opinion_id"))
	if err != nil {
		panic(err)
	}
	redisKey := fmt.Sprintf("opinion_%d", opinionId)
	opinion.Id = opinionId
	opinion.Created = time.Now().Unix()

	_, err = redisConn.Do("GEOADD", "opinions", opinion.Longitude, opinion.Latitude, redisKey)
	if err != nil {
		panic(err)
	}

	_, err = redisConn.Do("HMSET", redis.Args{redisKey}.AddFlat(opinion)...)
	if err != nil {
		panic(err)
	}
	redisConn.Do("EXPIRE", redisKey, opinion.TTL)

	return "ok", http.StatusOK
}

func getOpinionsNearMeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.NotFound(w, r)
	} else {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}

		var location Location
		err = json.Unmarshal(data, &location)
		fmt.Println(location)
		if err != nil {
			panic(err)
		}
		if location.Longitude == 0 || location.Latitude == 0 || location.Radius == 0 {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			response, status := getOpinionsNearMe(location.Radius, location.Longitude, location.Latitude)
			w.WriteHeader(status)
			w.Write([]byte(response))
		}
	}
}

func getOpinionsNearMe(radius int, longitude float64, latitude float64) (string, int) {
	opinions, err := redis.Values(redisConn.Do("GEORADIUS", "opinions", longitude, latitude, radius, "km", "WITHDIST", "ASC"))
	if err != nil {
		panic(err)
	}

	resultOpinions := make([]Opinion, len(opinions))

	for i, opinion := range opinions {
		opinionWithDist, err := redis.Strings(opinion, nil)
		if err != nil {
			panic(err)
		}
		opinionId, dist := opinionWithDist[0], opinionWithDist[1]
		if isOpinionAlive(opinionId) {
			redisOpinion, err := redis.Values(redisConn.Do("HGETALL", opinionId))
			if err != nil {
				panic(err)
			}

			var resultOpinion Opinion
			err = redis.ScanStruct(redisOpinion, &resultOpinion)
			if err != nil {
				panic(err)
			}
			resultDist, err := strconv.ParseFloat(dist, 32)
			if err != nil {
				panic(err)
			}
			resultOpinion.Dist = float32(resultDist)
			resultTTL, err := redis.Int(redisConn.Do("TTL", opinionId))
			resultOpinion.TTL = resultTTL

			resultOpinions[i] = resultOpinion
		} else {
			redisConn.Do("ZREM", "opinions", opinionId)
		}
	}

	response, err := json.Marshal(resultOpinions)
	return string(response), http.StatusOK
}

func isOpinionAlive(opinionId string) bool {
	ttl, err := redis.Int(redisConn.Do("TTL", opinionId))
	if err != nil {
		panic(err)
	}

	return ttl != -2
}