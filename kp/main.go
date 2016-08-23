package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/MediaMath/kafka-proxy"

	"gopkg.in/urfave/cli.v1"
)

func main() {
	app := cli.NewApp()
	app.Name = "kp"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "host",
			EnvVar: "KAFKA_REST_URL",
			Usage:  "url to the kafka rest url",
		},
		cli.BoolFlag{
			Name:  "pretty",
			Usage: "pretty print output",
		},
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "be chatty",
		},
		cli.StringFlag{
			Name:   "format",
			EnvVar: "KAFKA_REST_FORMAT",
			Value:  "avro",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "get",
			Usage:  "kp get -p 1 -t foo --from 6 --count 10",
			Action: get,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "topic,t",
				},
				cli.IntFlag{
					Name:  "partition,p",
					Usage: "partition to get from",
					Value: 0,
				},
				cli.Int64Flag{
					Name:  "from",
					Value: 0,
				},
				cli.IntFlag{
					Name:  "count",
					Value: 1,
				},
			},
		},
	}

	app.Run(os.Args)
}

func get(c *cli.Context) error {
	url := c.GlobalString("host")
	format := c.GlobalString("format")
	pretty := c.GlobalBool("pretty")
	verbose := c.GlobalBool("verbose")

	topic := c.String("topic")
	partition := c.Int("partition")
	from := c.Int64("from")
	count := c.Int("count")

	messages, err := proxy.Consume(http.DefaultClient, url, topic, int32(partition), from, count, proxy.Format(format))

	if err != nil && verbose {
		req, _ := proxy.ConsumeRequest(url, topic, int32(partition), from, count, proxy.Format(format))
		if req != nil {
			log.Printf("%v", req.URL)
		}
	}

	if err != nil {
		return err
	}

	var b []byte
	if pretty {
		b, err = json.MarshalIndent(messages, "", "    ")
	} else {
		b, err = json.Marshal(messages)
	}

	if err != nil {
		return err
	}

	fmt.Printf("%s", b)
	return nil
}
