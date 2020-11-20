namespace GraphBLAS.FSharp

open GraphBLAS.FSharp.Predefined
open OpenCLContext

module Algorithms =

    // push strategy
    let levelBFS (matrix: Matrix<bool>) (source: int) : Vector<int> =
        let vertexCount = matrix.RowCount
        let levels = DenseVector(Array.zeroCreate vertexCount, IntegerMonoid.plus)
        let frontier = SparseVector(vertexCount, [])
        frontier.Assign(Scalar true, source)

        let mutable currentLevel = 1
        while !> (frontier.Reduce BooleanMonoid.any) && currentLevel <= vertexCount do
            levels.Assign(Scalar currentLevel, Mask1D frontier)
            frontier.VxmInplace matrix (Complemented1D levels) BooleanSemiring.anyAll
            currentLevel <- currentLevel + 1

        upcast levels

    let parentBFS (matrix: Matrix<bool>) (source: int) : Vector<int> =
        let vertexCount = matrix.RowCount
        let id = DenseVector(Array.init vertexCount id, IntegerMonoid.plus)
        let frontier = SparseVector(vertexCount, [])
        frontier.Assign(Scalar source, source)
        let parents = SparseVector(vertexCount, [])
        parents.Assign(Scalar -1, source)

        for i in 0 .. vertexCount - 1 do
            frontier.Assign(frontier.Vxm matrix (Complemented1D parents) IntegerSemiring.minFirst , Complemented1D parents)
            parents.Assign(frontier, Mask1D frontier)
            frontier.Assign(id, Mask1D frontier)

        upcast parents
