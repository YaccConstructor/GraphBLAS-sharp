module EWiseAddTests

open Expecto
open FsCheck
open GraphBLAS.FSharp
open MathNet.Numerics
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GlobalContext
open TypeShape.Core
open GraphBLAS.FSharp.Tests
open System

type OperationParameter =
    | MatrixFormatParam of MatrixBackendFormat
    | MaskTypeParam of MaskType

type OperationCase = {
    MatrixCase: MatrixBackendFormat
    MaskCase: MaskType
}

let testCases =
    [
        Utils.listOfUnionCases<MatrixBackendFormat> |> List.map MatrixFormatParam
        Utils.listOfUnionCases<MaskType> |> List.map MaskTypeParam
    ]
    |> Utils.cartesian
    |> List.map
        (fun list ->
            let (MatrixFormatParam marixFormat) = list.[0]
            let (MaskTypeParam maskType) = list.[1]
            {
                MatrixCase = marixFormat
                MaskCase = maskType
            }
        )

let createMatrix<'a when 'a : struct and 'a : equality> matrixFormat args =
    match matrixFormat with
    | CSR ->
        Activator.CreateInstanceGeneric<CSRMatrix<_>>(
            Array.singleton typeof<'a>, args
        )
        |> unbox<CSRMatrix<'a>>
        :> Matrix<'a>
    | COO ->
        Activator.CreateInstanceGeneric<COOMatrix<_>>(
            Array.singleton typeof<'a>, args
        )
        |> unbox<COOMatrix<'a>>
        :> Matrix<'a>

let createCSR<'a when 'a : struct and 'a : equality> = createMatrix<'a> CSR
let createCOO<'a when 'a : struct and 'a : equality> = createMatrix<'a> COO

type PrimitiveType =
    | Float32
    | Bool

type PairOfSparseMatrices =
    static member Float32Type() =
        Arb.fromGen <| Generators.pairOfSparseMatricesGenerator
            Arb.generate<float32>
            0.f
            ((=) 0.f)

    static member BoolType() =
        Arb.fromGen <| Generators.pairOfSparseMatricesGenerator
            Arb.generate<bool>
            false
            ((=) false)

let meaning primitiveType =
    match primitiveType with
    | Float32 -> typeof<float32>
    | Bool -> typeof<bool>

let printer primitiveType =
    match primitiveType with
    | Float32 -> "float32"
    | Bool -> "bool"

let checkConcrete (testCase: OperationCase) (primitiveType: PrimitiveType) =
    let config = { FsCheckConfig.defaultConfig with arbitrary = [typeof<PairOfSparseMatrices>] }
    let systemType = meaning primitiveType
    let prettyType = printer primitiveType
    let shape = TypeShape.Create systemType
    shape.Accept { new ITypeVisitor<Test> with
        member this.Visit<'a>() =
            testPropertyWithConfig config (sprintf "On type %s" prettyType) <|
                fun (matrixA: 'a[,]) (matrixB: 'a[,]) ->
                    // let sparseA = createMatrix<'a> testCase.MatrixCase [|
                    //     box matrixA
                    //     box ((=) 0.)
                    // |]
                    // let sparseB = createMatrix<'a> testCase.MatrixCase [|
                    //     box matrixB
                    //     box ((=) 0.)
                    // |]

                    let a = Matrix.ofArray2D matrixA ((=) 0.)

                    // let result =
                    //     opencl {
                    //         return! (sparseA + sparseB) mask stdSemiring
                    //     } |> oclContext.RunSync

                    ()
                    // let a = LinearAlgebra.DenseMatrix.ofArray2 matrixA
                    // let b = LinearAlgebra.DenseVector.ofArray matrixB
                    // let c = b * a
                    // let elementWiseDifference =
                    //     (result |> Vector.toSeq, c.AsArray() |> Seq.ofArray)
                    //     ||>  Seq.zip
                    //     |> Seq.map (fun (a, b) -> a - b)

                    // Expect.all
                    //     elementWiseDifference
                    //     (fun diff -> abs diff < Accuracy.medium.absolute)
                    //     (sprintf "%A @ %A = %A\n case:\n %A" vector matrix result case)
    }

let testsI =
    testCases
    |> List.collect
        (fun case ->
            [
                Utils.listOfUnionCases<PrimitiveType>
                |> List.map (checkConcrete case)
                |> testList "Operation correctness"
            ]
        )
    |> testList "EWiseAdd Tests"
