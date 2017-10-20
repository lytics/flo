package merger

// Merger for merging a and b into something new.
type Merger func(a, b interface{}) (interface{}, error)

// ManyMerger is like merge but for values from two
// slices which need to be merged.
type ManyMerger func(as, bs []interface{}) ([]interface{}, error)

// Cons merges by appending values togeter. For example, the
// merger of []string{"a", "b"} and []string{"c", "d"} is
// the slice []string{"a", "b", "c", "d"}
func Cons() ManyMerger {
	return func(as, bs []interface{}) ([]interface{}, error) {
		if bs == nil {
			return as, nil
		}
		bs = append(bs, as...)
		return bs, nil
	}
}

// Zip merges by using function f to merge values pairwise from
// slices as and bs.
//
// Example:
//
//     as = [ 1 2 3 ]
//     bs = [ 7 8 9 10 ]
//
//     result = [ f(1, 7), f(2,8), f(3, 9) ]
//
// Zip ignores values that have an index value larger than the
// shortest slice, in the example above the value 10.
//
func Zip(f Merger) ManyMerger {
	return nil
}

// Fold merges by using function f to merge values down to a single value.
// The retruned slice will either be nil or have a length of one. First
// value pairs from as are folded into a single value, call it a. Then
// value pairs from bs are folded into a single value, call it b. Then
// a and b are merged into a single result.
//
// Example:
//
//        as = [ 1 2 3 ]
//        bs = [ 7 8 9 ]
//
//         f = +
//
//         a = f(f(1, 2), 3)
//           = f(3, 3)
//           = 6
//
//         b = f(f(7, 8), 9)
//           = f(15, 9)
//           = 24
//
//    result = f(a, b)
//           = 30
//
func Fold(f Merger) ManyMerger {
	return func(as, bs []interface{}) ([]interface{}, error) {
		a, err := foldSlice(bs, f)
		if err != nil {
			return nil, err
		}
		b, err := foldSlice(as, f)
		if err != nil {
			return nil, err
		}
		c, err := foldSlice([]interface{}{a, b}, f)
		if err != nil {
			return nil, err
		}
		return []interface{}{c}, nil
	}
}

// foldSlice folds the values into a single value, using
// the given merger function.
func foldSlice(vs []interface{}, f Merger) (interface{}, error) {
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
