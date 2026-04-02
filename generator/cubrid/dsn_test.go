package cubrid

import "testing"

func TestExtractDBName(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		want    string
		wantErr bool
	}{
		{"full DSN", "cubrid://dba:pass@localhost:33000/demodb", "demodb", false},
		{"empty password", "cubrid://dba:@localhost:33000/testdb", "testdb", false},
		{"with params", "cubrid://dba:@localhost:33000/mydb?auto_commit=true", "mydb", false},
		{"no scheme", "dba:pass@localhost:33000/demodb", "demodb", false},
		{"empty DSN", "", "", true},
		{"no database", "cubrid://dba:@localhost:33000/", "", true},
		{"no slash", "cubrid://dba:@localhost:33000", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractDBName(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractDBName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("extractDBName() = %q, want %q", got, tt.want)
			}
		})
	}
}
