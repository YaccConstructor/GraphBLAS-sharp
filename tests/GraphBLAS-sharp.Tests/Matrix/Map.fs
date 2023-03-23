module GraphBLAS.FSharp.Tests.Backend.Matrix.Map

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClContext
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

let createTestMap case (zero: 'a) op isEqual opQ map =
    let getCorrectnessTestName = getCorrectnessTestName case

    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    let map = map context opQ wgSize

    let toCOO = Matrix.toCOO context wgSize

    case
    |> correctnessGenericTest zero op map toCOO isEqual q
    |> testPropertyWithConfig config (getCorrectnessTestName $"{typeof<'a>}")

let testFixturesMapNot case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      createTestMap case false not (=) ArithmeticOperations.notQ Matrix.map ]

let notTests =
    operationGPUTests "Backend.Matrix.map not tests" testFixturesMapNot

let testFixturesMapAdd case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let addFloat64Q =
          ArithmeticOperations.mkOpWithConst 0.0 (+) 10.0

      let addFloat32Q =
          ArithmeticOperations.mkOpWithConst 0.0f (+) 10.0f

      let addByte =
          ArithmeticOperations.mkOpWithConst 0uy (+) 10uy

      if Utils.isFloat64Available context.ClDevice then
          createTestMap case 0.0 ((+) 10.0) Utils.floatIsEqual addFloat64Q Matrix.map

      createTestMap case 0.0f ((+) 10.0f) Utils.float32IsEqual addFloat32Q Matrix.map
      createTestMap case 0uy ((+) 10uy) (=) addByte Matrix.map ]

let addTests =
    operationGPUTests "Backend.Matrix.map add tests" testFixturesMapAdd

let testFixturesMapMul case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let mulFloat64Q =
          ArithmeticOperations.mkOpWithConst 0.0 (*) 10.0

      let mulFloat32Q =
          ArithmeticOperations.mkOpWithConst 0.0f (*) 10.0f

      let mulByte =
          ArithmeticOperations.mkOpWithConst 0uy (*) 10uy

      if Utils.isFloat64Available context.ClDevice then
          createTestMap case 0.0 ((*) 10.0) Utils.floatIsEqual mulFloat64Q Matrix.map

      createTestMap case 0.0f ((*) 10.0f) Utils.float32IsEqual mulFloat32Q Matrix.map
      createTestMap case 0uy ((*) 10uy) (=) mulByte Matrix.map ]

let mulTests =
    operationGPUTests "Backend.Matrix.map mul tests" testFixturesMapMul
