package split_and_commp

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/anjor/carlet"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

var Cmd = &cli.Command{
	Name:    "split-and-commp",
	Usage:   "Split CAR and calculate commp",
	Aliases: []string{"sac"},
	Action:  splitAndCommpAction,
	Flags:   splitAndCommpFlags,
}

var splitAndCommpFlags = []cli.Flag{
	&cli.IntFlag{
		Name:     "size",
		Aliases:  []string{"s"},
		Required: true,
		Usage:    "Target size in bytes to chunk CARs to.",
	},
	&cli.StringFlag{
		Name:     "output",
		Aliases:  []string{"o"},
		Required: true,
		Usage:    "optional output filename prefix for car files.",
	},
	&cli.StringFlag{
		Name:     "metadata",
		Aliases:  []string{"m"},
		Required: false,
		Usage:    "optional metadata file name. Defaults to __metadata.csv",
		Value:    "__metadata.csv",
	},
}

func splitAndCommpAction(c *cli.Context) error {
	fi, err := getReader(c)
	if err != nil {
		return err
	}

	size := c.Int("size")
	output := c.String("output")
	meta := c.String("metadata")

	var filenamePrefix string

	if output != "" {
		filenamePrefix = fmt.Sprintf("%s-", output)
	}

	carPieceFilesMeta, err := carlet.SplitAndCommp(fi, size, filenamePrefix)
	if err != nil {
		return err
	}

	metaFile, err := os.Create(meta)
	if err != nil {
		return err
	}
	defer metaFile.Close()
	if err != nil {
		return err
	}

	csvWriter := csv.NewWriter(metaFile)
	err = csvWriter.Write([]string{
		"timestamp",
		"car file",
		"piece cid",
		"padded piece size",
		"header size",
		"content size",
	})
	if err != nil {
		return err
	}
	defer csvWriter.Flush()
	for _, cf := range carPieceFilesMeta.CarPieces {
		err = csvWriter.Write([]string{
			time.Now().Format(time.RFC3339),
			cf.Name,
			cf.CommP.String(),
			strconv.FormatUint(cf.PaddedSize, 10),
			strconv.FormatUint(cf.HeaderSize, 10),
			strconv.FormatUint(cf.ContentSize, 10),
		})
		if err != nil {
			return fmt.Errorf("failed to write csv row: %s", err)
		}
	}
	{
		// save also as yaml
		yamlFilename := strings.TrimSuffix(meta, filepath.Ext(meta)) + ".yaml"
		yamlFile, err := os.Create(yamlFilename)
		if err != nil {
			panic(fmt.Errorf("failed to create yaml metadata file: %s", err))
		}
		defer yamlFile.Close()

		// write all the car files as yaml

		yamlWriter := yaml.NewEncoder(yamlFile)
		var carFilesYaml struct {
			CarPiecesMeta *carlet.CarPiecesAndMetadata `yaml:"car_pieces_meta"`
		}
		carFilesYaml.CarPiecesMeta = carPieceFilesMeta
		err = yamlWriter.Encode(carFilesYaml)
		if err != nil {
			panic(fmt.Errorf("failed to write yaml: %s", err))
		}
	}
	return nil
}

func getReader(c *cli.Context) (io.Reader, error) {
	if c.Args().Present() {
		path := c.Args().First()
		fi, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return fi, nil
	}
	return os.Stdin, nil
}
