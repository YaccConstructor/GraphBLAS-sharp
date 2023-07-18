module GraphBLAS.FSharp.Tests.Backend.Matrix.Map

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions

let logger = Log.create "Map.Tests"

let config = Utils.defaultConfig
let wgSize = Utils.defaultWorkGroupSize

let getCorrectnessTestName case datatype =
    $"Correctness on %s{datatype}, %A{case}"

let checkResult isEqual op zero (baseMtx: 'a [,]) (actual: Matrix<'a>) =
    let rows = Array2D.length1 baseMtx
    let columns = Array2D.length2 baseMtx
    Expect.equal columns actual.ColumnCount "The number of columns should be the same."
    Expect.equal rows actual.RowCount "The number of rows should be the same."

    let expected2D = Array2D.create rows columns zero

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            expected2D.[i, j] <- op baseMtx.[i, j]

    let actual2D = Array2D.create rows columns zero

    match actual with
    | Matrix.COO actual ->
        for i in 0 .. actual.Columns.Length - 1 do
            if isEqual zero actual.Values.[i] then
                failwith "Resulting zeroes should be filtered."

            actual2D.[actual.Rows.[i], actual.Columns.[i]] <- actual.Values.[i]
    | _ -> failwith "Resulting matrix should be converted to COO format."

    "Arrays must be the same"
    |> Utils.compare2DArrays isEqual actual2D expected2D

let correctnessGenericTest
    zero
    op
    (addFun: MailboxProcessor<_> -> AllocationFlag -> ClMatrix<'a> -> ClMatrix<'b>)
    toCOOFun
    (isEqual: 'a -> 'a -> bool)
    q
    (case: OperationCase<MatrixFormat>)
    (matrix: 'a [,])
    =
    match case.Format with
    | LIL -> ()
    | _ ->
        let mtx =
            Utils.createMatrixFromArray2D case.Format matrix (isEqual zero)

        if mtx.NNZ > 0 then
            try
                let m = mtx.ToDevice case.TestContext.ClContext

                let res = addFun q HostInterop m

                m.Dispose q

                let (cooRes: ClMatrix<'a>) = toCOOFun q HostInterop res
                let actual = cooRes.ToHost q

                cooRes.Dispose q
                res.Dispose q

                logger.debug (
                    eventX "Actual is {actual}"
                    >> setField "actual" (sprintf "%A" actual)
                )

                checkResult isEqual op zero matrix actual
            with
            | ex when ex.Message = "InvalidBufferSize" -> ()
            | ex -> raise ex

let createTestMap case (zero: 'a) (constant: 'a) binOp isEqual opQ =
    let getCorrectnessTestName = getCorrectnessTestName case

    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    let unaryOp = binOp constant
    let unaryOpQ = opQ zero constant

    let map = Matrix.map unaryOpQ context wgSize

    let toCOO = Matrix.toCOO context wgSize

    case
    |> correctnessGenericTest zero unaryOp map toCOO isEqual q
    |> testPropertyWithConfig config (getCorrectnessTestName $"{typeof<'a>}")

let testFixturesMapNot case =
    [ let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTestMap case false true (fun _ -> not) (=) (fun _ _ -> ArithmeticOperations.notOption) ]

let notTests =
    operationGPUTests "not" testFixturesMapNot

let testFixturesMapAdd case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTestMap case 0 10 (+) (=) ArithmeticOperations.addLeftConst

      if Utils.isFloat64Available context.ClDevice then
          createTestMap case 0.0 10.0 (+) Utils.floatIsEqual ArithmeticOperations.addLeftConst

      createTestMap case 0.0f 10.0f (+) Utils.float32IsEqual ArithmeticOperations.addLeftConst

      createTestMap case 0uy 10uy (+) (=) ArithmeticOperations.addLeftConst ]

let addTests =
    operationGPUTests "add" testFixturesMapAdd

let testFixturesMapMul case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTestMap case 0 10 (*) (=) ArithmeticOperations.mulLeftConst

      if Utils.isFloat64Available context.ClDevice then
          createTestMap case 0.0 10.0 (*) Utils.floatIsEqual ArithmeticOperations.mulLeftConst

      createTestMap case 0.0f 10.0f (*) Utils.float32IsEqual ArithmeticOperations.mulLeftConst

      createTestMap case 0uy 10uy (*) (=) ArithmeticOperations.mulLeftConst ]

let mulTests =
    operationGPUTests "mul" testFixturesMapMul

let allTests =
    testList "Map" [ addTests; mulTests; notTests ]
