namespace GraphBLAS.FSharp.Tests

open Expecto
open FsCheck
open System
open GraphBLAS.FSharp

type OperationCase = {
    VectorCase: VectorType
    MatrixCase: MatrixType
    MaskCase: MaskType
}

type MatrixMultiplicationPair =
    static member DimensionGen2 () =
        fun size ->
            Gen.choose (0, size |> float |> sqrt |> int)
            |> Gen.two
        |> Gen.sized
        |> Arb.fromGen

    static member DimensionGen3 () =
        fun size ->
            Gen.choose (0, size |> float |> sqrt |> int)
            |> Gen.three
        |> Gen.sized
        |> Arb.fromGen

    static member FloatSparseMatrixVectorPair () =
        fun size ->
            let floatSparseGenerator =
                Gen.oneof [
                    Arb.Default.NormalFloat () |> Arb.toGen |> Gen.map float
                    Gen.constant 0.
                ]

            let dimGenerator =
                Gen.choose (0, size |> float |> sqrt |> int)
                |> Gen.two

            gen {
                let! (rows, cols) = dimGenerator
                let! matrix = floatSparseGenerator |> Gen.array2DOfDim (rows, cols)
                let! vector = floatSparseGenerator |> Gen.arrayOfLength cols
                return (matrix, vector)
            }
            |> Gen.filter (fun (matrix, vector) -> matrix.Length <> 0 && vector.Length <> 0)
            |> Gen.filter (fun (matrix, vector) ->
                matrix |> Seq.cast<float> |> Seq.exists (fun elem -> abs elem > System.Double.Epsilon) &&
                vector |> Seq.cast<float> |> Seq.exists (fun elem -> abs elem > System.Double.Epsilon))
        |> Gen.sized
        |> Arb.fromGen

module VxmTests =
    open Backend

    let config = { FsCheckConfig.defaultConfig with arbitrary = [typeof<MatrixMultiplicationPair>] }

    let testCases =
        [
            typeof<VectorType>
            typeof<MatrixType>
            typeof<MaskType>
        ]
        |> List.map Utils.enumValues
        |> Utils.cartesian
        |> List.map (fun list ->
            {
                VectorCase = enum<VectorType> list.[0]
                MatrixCase = enum<MatrixType> list.[1]
                MaskCase = enum<MaskType> list.[2]
            })

    [<Tests>]
    let vxmTestList =
        testList "Vector-matrix multiplication tests" (
            List.collect (fun case ->
                let matrixBackend =
                    match case.MatrixCase with
                    | MatrixType.CSR -> CSR
                    | _ -> failwith "Not Implemented"
                // let vectorConstructor =
                //     match case.VectorCase with
                //     | VectorType.Sparse ->

                [
                    testPropertyWithConfig config "Dimensional mismatch should raise an exception" <|
                        // тут просто размерности генерить и создавать пустые объекты
                        fun m n k ->
                            let emptyMatrix = Matrix.ZeroCreate(m, n, matrixBackend)
                            let emptyVector = Vector.Dense(Predefined.FloatMonoid.add)

                            Expect.throwsT
                                (fun () -> (emptyVector .@ emptyMatrix) Mask1D.none Predefined.FloatSemiring.addMult |> ignore)
                                (sprintf "Argument has invalid dimension. Need %i, but given %i" k m )

                    testPropertyWithConfig config "Operation should have correct semantic" <|
                        fun (matrix: float[,]) (vector: float[,]) -> ()

                    ptestPropertyWithConfig config "Explicit zeroes after operation should be dropped" <|
                        fun a b -> a + b = b + a
                ]
            ) testCases
        )
