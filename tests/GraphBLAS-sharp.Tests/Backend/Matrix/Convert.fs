module GraphBLAS.FSharp.Tests.Backend.Matrix.Convert

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Convert.Tests"

let config = Utils.defaultConfig

let workGroupSize = Utils.defaultWorkGroupSize

let context = defaultContext.ClContext

let q = defaultContext.Queue

q.Error.Add(fun e -> failwithf "%A" e)

let makeTest context q formatFrom formatTo convertFun isZero (array: 'a [,]) =
    let mtx =
        Utils.createMatrixFromArray2D formatFrom array isZero

    if mtx.NNZ > 0 then
        let actual =
            let mBefore = mtx.ToDevice context
            let mAfter: ClMatrix<'a> = convertFun q HostInterop mBefore
            let res = mAfter.ToHost q
            mBefore.Dispose q
            mAfter.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected =
            Utils.createMatrixFromArray2D formatTo array isZero

        "Row count should be the same"
        |> Expect.equal actual.RowCount (Array2D.length1 array)

        "Column count should be the same"
        |> Expect.equal actual.ColumnCount (Array2D.length2 array)

        "Matrices should be equal"
        |> Expect.equal actual expected

let createTest<'a when 'a: struct and 'a: equality> convertFun formatTo (isZero: 'a -> bool) =
    let convertFun =
        convertFun context Utils.defaultWorkGroupSize

    Utils.listOfUnionCases<MatrixFormat>
    |> List.map
        (fun formatFrom ->
            makeTest context q formatFrom formatTo convertFun isZero
            |> testPropertyWithConfig config $"test on %A{typeof<'a>} from %A{formatFrom}")

let testFixtures formatTo =
    match formatTo with
    | COO ->
        [ createTest<int> Matrix.toCOO formatTo ((=) 0)
          createTest<bool> Matrix.toCOO formatTo ((=) false) ]
    | CSR ->
        [ createTest<int> Matrix.toCSR formatTo ((=) 0)
          createTest<bool> Matrix.toCSR formatTo ((=) false) ]
    | CSC ->
        [ createTest<int> Matrix.toCSC formatTo ((=) 0)
          createTest<bool> Matrix.toCSC formatTo ((=) false) ]
    | LIL ->
        [ createTest<int> Matrix.toLIL formatTo ((=) 0)
          createTest<bool> Matrix.toLIL formatTo ((=) false) ]
    |> List.concat
    |> testList $"%A{formatTo}"

let tests =
    Utils.listOfUnionCases<MatrixFormat>
    |> List.map testFixtures
    |> testList "Convert"
