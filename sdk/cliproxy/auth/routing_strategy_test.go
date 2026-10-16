package auth

import "testing"

func TestNormalizeRoutingStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		want   string
		wantOK bool
	}{
		{name: "default empty", input: "", want: "round-robin", wantOK: true},
		{name: "round robin alias", input: "rr", want: "round-robin", wantOK: true},
		{name: "weighted round robin alias", input: "wrr", want: "weighted-round-robin", wantOK: true},
		{name: "fill first alias", input: "ff", want: "fill-first", wantOK: true},
		{name: "invalid", input: "bogus", want: "", wantOK: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := NormalizeRoutingStrategy(tt.input)
			if ok != tt.wantOK {
				t.Fatalf("NormalizeRoutingStrategy(%q) ok = %t, want %t", tt.input, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("NormalizeRoutingStrategy(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSelectorForRoutingStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantType any
	}{
		{name: "default empty", input: "", wantType: &RoundRobinSelector{}},
		{name: "weighted round robin alias", input: "wrr", wantType: &WeightedRoundRobinSelector{}},
		{name: "fill first alias", input: "ff", wantType: &FillFirstSelector{}},
		{name: "invalid defaults round robin", input: "bogus", wantType: &RoundRobinSelector{}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := SelectorForRoutingStrategy(tt.input)
			switch tt.wantType.(type) {
			case *RoundRobinSelector:
				if _, ok := got.(*RoundRobinSelector); !ok {
					t.Fatalf("SelectorForRoutingStrategy(%q) = %T, want *RoundRobinSelector", tt.input, got)
				}
			case *WeightedRoundRobinSelector:
				if _, ok := got.(*WeightedRoundRobinSelector); !ok {
					t.Fatalf("SelectorForRoutingStrategy(%q) = %T, want *WeightedRoundRobinSelector", tt.input, got)
				}
			case *FillFirstSelector:
				if _, ok := got.(*FillFirstSelector); !ok {
					t.Fatalf("SelectorForRoutingStrategy(%q) = %T, want *FillFirstSelector", tt.input, got)
				}
			default:
				t.Fatalf("unexpected wantType %T", tt.wantType)
			}
		})
	}
}
