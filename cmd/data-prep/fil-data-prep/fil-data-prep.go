package fil_data_prep

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anjor/anelace"
	"github.com/anjor/carlet"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

var Cmd = &cli.Command{
	Name:    "fil-data-prep",
	Usage:   "end to end data prep",
	Aliases: []string{"dp"},
	Action:  filDataPrep,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Required: false,
			Usage:    "optional output filename prefix for car filename.",
		},
		&cli.IntFlag{
			Name:     "size",
			Aliases:  []string{"s"},
			Required: false,
			Value:    2 << 20,
			Usage:    "Target size in bytes to chunk CARs to.",
		},
		&cli.StringFlag{
			Name:     "metadata",
			Aliases:  []string{"m"},
			Required: false,
			Value:    "__metadata.csv",
			Usage:    "metadata file name. ",
		},
		&cli.BoolFlag{
			Name:     "dry-run",
			Aliases:  []string{"d"},
			Required: false,
			Usage:    "optional dry run. Do not write split CARs to disk (but still write metadata).",
			Value:    false,
		},
	},
}

func filDataPrep(c *cli.Context) error {
	if !c.Args().Present() {
		return fmt.Errorf("expected some data to be processed, found none")
	}

	var fileReaders []io.Reader
	var files []string
	paths := c.Args().Slice()

	for _, path := range paths {
		fs, frs, err := getAllFileReadersFromPath(path)
		if err != nil {
			return err
		}

		files = append(files, fs...)
		fileReaders = append(fileReaders, frs...)
	}

	wg := sync.WaitGroup{}
	wg.Add(3)

	rerr, werr := io.Pipe()
	rout, wout := io.Pipe()

	anl, errs := anelace.NewAnelaceWithWriters(werr, wout)
	if errs != nil {
		return fmt.Errorf("unexpected error: %s", errs)
	}
	anl.SetMultipart(true)

	go func() {
		defer wg.Done()
		defer werr.Close()
		if err := anl.ProcessReader(io.MultiReader(fileReaders...), nil); err != nil {
			fmt.Printf("process reader error: %s", err)
		}
	}()

	var rs []roots
	var rcid cid.Cid
	go func() {
		defer wg.Done()
		defer wout.Close()

		rs = getRoots(rerr)

		tr := constructTree(files, rs)
		nodes := getDirectoryNodes(tr)

		if len(nodes) == 1 || len(paths) > 1 { // len(nodes) = 1 means a file was passed as input
			// use fake root directory if multiple args.
			// If there are nested paths it will wrap all the intermediate directories up in the fake root
			rcid = nodes[0].Cid()
			writeNode(nodes, wout)
		} else {
			path := paths[0]

			// Need to do this to handle nested paths, where the root cid should be the actual final directory
			// for example, if the input is /opt/data/data_dir, the root cid should correspond to data_dir and not to /
			splitPath := strings.Split(path, "/")
			idx := len(splitPath)
			rcid = nodes[idx].Cid()

			writeNode(nodes[idx:], wout)
		}
	}()

	o := c.String("output")
	meta := c.String("metadata")
	s := c.Int("size")
	dryRun := c.Bool("dry-run")

	var filenamePrefix string
	if o != "" {
		// Add a dash to separate prefix from filename
		// note: we only do this when prefix specified, otherwise filename will begin with "-", which can cause problem with some fs operations as it is interpreted as a flag
		filenamePrefix = fmt.Sprintf("%s-", o)
	}

	go func() {
		defer wg.Done()

		var carPieceFilesMeta *carlet.CarPiecesAndMetadata
		var err error
		if dryRun {
			carPieceFilesMeta, err = carlet.SplitAndCommpDryRun(rout, s, filenamePrefix)
		} else {
			carPieceFilesMeta, err = carlet.SplitAndCommp(rout, s, filenamePrefix)
		}
		if err != nil {
			panic(fmt.Errorf("split and commp failed : %s", err))
		}

		metaFile, err := os.Create(meta)
		if err != nil {
			panic(fmt.Errorf("failed to create metadata file: %s", err))
		}
		defer metaFile.Close()

		csvWriter := csv.NewWriter(metaFile)
		err = csvWriter.Write([]string{
			"timestamp",
			"car file",
			"root_cid",
			"piece cid",
			"padded piece size",
			"header size",
			"content size",
		})
		if err != nil {
			panic(fmt.Errorf("failed to write csv header: %s", err))
		}
		defer csvWriter.Flush()
		for _, cf := range carPieceFilesMeta.CarPieces {
			err = csvWriter.Write([]string{
				time.Now().UTC().Format(time.RFC3339),
				cf.Name,
				rcid.String(),
				cf.CommP.String(),
				strconv.FormatUint(cf.PaddedSize, 10),
				strconv.FormatUint(cf.HeaderSize, 10),
				strconv.FormatUint(cf.ContentSize, 10),
			})
			if err != nil {
				panic(fmt.Errorf("failed to write csv row: %s", err))
			}
		}
		{
			// save also as yaml, which will include the whole car pieces metadata (including the original car header)
			yamlFilename := strings.TrimSuffix(meta, filepath.Ext(meta)) + ".yaml"
			yamlFile, err := os.Create(yamlFilename)
			if err != nil {
				panic(fmt.Errorf("failed to create yaml metadata file: %s", err))
			}
			defer yamlFile.Close()

			yamlWriter := yaml.NewEncoder(yamlFile)
			var carFilesYaml struct {
				RootCid       string                       `yaml:"root_cid"`
				CarPiecesMeta *carlet.CarPiecesAndMetadata `yaml:"car_pieces_meta"`
			}
			carFilesYaml.RootCid = rcid.String()
			carFilesYaml.CarPiecesMeta = carPieceFilesMeta
			err = yamlWriter.Encode(carFilesYaml)
			if err != nil {
				panic(fmt.Errorf("failed to write yaml: %s", err))
			}
		}
	}()

	wg.Wait()

	fmt.Printf("root cid = %s\n", rcid)

	return nil
}

func writeNode(nodes []*merkledag.ProtoNode, wout *io.PipeWriter) {
	var c, sizeVi []byte
	for _, nd := range nodes {
		c = []byte(nd.Cid().KeyString())
		d := nd.RawData()

		sizeVi = appendVarint(sizeVi[:0], uint64(len(c))+uint64(len(d)))

		if _, err := wout.Write(sizeVi); err == nil {
			if _, err := wout.Write(c); err == nil {
				if _, err := wout.Write(d); err != nil {
					fmt.Printf("failed to write car: %s\n", err)
				}
			}
		}
	}
}

func getRoots(rerr *io.PipeReader) []roots {
	var rs []roots
	bs, _ := io.ReadAll(rerr)
	e := string(bs)
	els := strings.Split(e, "\n")
	for _, el := range els {
		if el == "" {
			continue
		}
		var r roots
		err := json.Unmarshal([]byte(el), &r)
		if err != nil {
			fmt.Printf("failed to unmarshal json: %s\n", el)
		}
		rs = append(rs, r)
	}
	return rs
}
