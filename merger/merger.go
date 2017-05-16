package merger

// Merger for merging a and b into something new.
type Merger func(a, b interface{}) (interface{}, error)

// ManyMerger is like merge but for values from two
// slices which need to be merged.
type ManyMerger func(as, bs []interface{}) ([]interface{}, error)

// Cons merges by appending values togeter.
func Cons() ManyMerger {
	return func(as, bs []interface{}) ([]interface{}, error) {
		if bs == nil {
			return as, nil
		}
		bs = append(bs, as...)
		return bs, nil
	}
}

// Fold merges by using function f to merge values down to one.
func Fold(f Merger) ManyMerger {
	return func(as, bs []interface{}) ([]interface{}, error) {
		a, err := foldOne(bs, f)
		if err != nil {
			return nil, err
		}
		b, err := foldOne(as, f)
		if err != nil {
			return nil, err
		}
		c, err := foldOne([]interface{}{a, b}, f)
		if err != nil {
			return nil, err
		}
		return []interface{}{c}, nil
	}
}

func foldOne(vs []interface{}, f Merger) (interface{}, error) {
	if len(vs) == 0 {
		return nil, nil
	}
	if len(vs) == 1 {
		return vs[0], nil
	}
	v0 := vs[0]
	var err error
	for _, vn := range vs[1:] {
		v0, err = f(v0, vn)
		if err != nil {
			return nil, err
		}
	}
	return v0, nil
}
