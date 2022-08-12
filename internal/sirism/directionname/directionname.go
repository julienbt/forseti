package directionname

import (
	"encoding/xml"
	"fmt"
)

type DirectionName int

const (
	DirectionNameAller  DirectionName = 0
	DirectionNameRetour DirectionName = 1
)

func (dn *DirectionName) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var innerText string
	err := d.DecodeElement(&innerText, &start)
	if err != nil {
		return err
	}

	if innerText == "ALLER" {
		*dn = DirectionNameAller
		return nil
	} else if innerText == "RETOUR" {
		*dn = DirectionNameRetour
		return nil
	}

	return fmt.Errorf("the `DirectionName` is not well formatted: %s", innerText)
}
