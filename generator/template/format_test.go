package template

import "testing"

func Test_formatGolangComment(t *testing.T) {
	type args struct {
		comment string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "Empty string", args: args{comment: ""}, want: ""},
		{name: "Non-empty string", args: args{comment: "This is a comment"}, want: "// This is a comment"},
		{name: "String with control characters", args: args{comment: "This is a comment with control characters \x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f and text after"}, want: "// This is a comment with control characters  and text after"},
		{name: "String with escape characters", args: args{comment: "This is a comment with escape characters \n\r\t and text after"}, want: "// This is a comment with escape characters  and text after"},
		{name: "String with unicode characters", args: args{comment: "This is a comment with unicode characters ₲鬼佬℧⇄↻ and text after"}, want: "// This is a comment with unicode characters ₲鬼佬℧⇄↻ and text after"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatGolangComment(tt.args.comment); got != tt.want {
				t.Errorf("formatGoLangComment() = %v, want %v", got, tt.want)
			}
		})
	}
}
