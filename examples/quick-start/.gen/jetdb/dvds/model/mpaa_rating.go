//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package model

import "errors"

type MpaaRating string

const (
	MpaaRating_G    MpaaRating = "G"
	MpaaRating_Pg   MpaaRating = "PG"
	MpaaRating_Pg13 MpaaRating = "PG-13"
	MpaaRating_R    MpaaRating = "R"
	MpaaRating_Nc17 MpaaRating = "NC-17"
)

func (e *MpaaRating) Scan(value interface{}) error {
	var enumValue string
	switch val := value.(type) {
	case string:
		enumValue = val
	case []byte:
		enumValue = string(val)
	default:
		return errors.New("jet: Invalid scan value for AllTypesEnum enum. Enum value has to be of type string or []byte")
	}

	switch enumValue {
	case "G":
		*e = MpaaRating_G
	case "PG":
		*e = MpaaRating_Pg
	case "PG-13":
		*e = MpaaRating_Pg13
	case "R":
		*e = MpaaRating_R
	case "NC-17":
		*e = MpaaRating_Nc17
	default:
		return errors.New("jet: Invalid scan value '" + enumValue + "' for MpaaRating enum")
	}

	return nil
}

func (e MpaaRating) String() string {
	return string(e)
}
