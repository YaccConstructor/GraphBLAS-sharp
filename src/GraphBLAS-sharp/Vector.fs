namespace GraphBLAS.FSharp

[<AbstractClass>]
type Vector<'a>() =
    abstract Length: int
    abstract AsArray: 'a[]

type DenseVector<'a>(vector: 'a[]) =
    inherit Vector<'a>()

    override this.Length = Array.length vector
    override this.AsArray = vector
