module GraphBLAS.FSharp.Tests.Backend.Matrix.Map2

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.TestCases
open Microsoft.FSharp.Collections
open Utils
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Elementwise.Tests"

let checkResult isEqual op zero (baseMtx1: 'a [,]) (baseMtx2: 'a [,]) (actual: Matrix<'a>) =
    let rows = Array2D.length1 baseMtx1
    let columns = Array2D.length2 baseMtx1
    Expect.equal columns actual.ColumnCount "The number of columns should be the same."
    Expect.equal rows actual.RowCount "The number of rows should be the same."

    let expected2D = Array2D.create rows columns zero

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            expected2D.[i, j] <- op baseMtx1.[i, j] baseMtx2.[i, j]

    let actual2D = Array2D.create rows columns zero

    match actual with
    | Matrix.COO actual ->
        for i in 0 .. actual.Rows.Length - 1 do
            if isEqual zero actual.Values.[i] then
                failwith "Resulting zeroes should be filtered."

            actual2D.[actual.Rows.[i], actual.Columns.[i]] <- actual.Values.[i]
    | _ -> failwith "Resulting matrix should be converted to COO format."

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            Expect.isTrue
                (isEqual actual2D.[i, j] expected2D.[i, j])
                $"Values should be the same. Actual is {actual2D.[i, j]}, expected {expected2D.[i, j]}."

let correctnessGenericTest
    zero
    op
    (addFun: MailboxProcessor<_> -> AllocationFlag -> ClMatrix<'a> -> ClMatrix<'a> -> ClMatrix<'c>)
    toCOOFun
    (isEqual: 'a -> 'a -> bool)
    q
    (case: OperationCase<MatrixFormat>)
    (leftMatrix: 'a [,], rightMatrix: 'a [,])
    =

    let mtx1 =
        createMatrixFromArray2D case.Format leftMatrix (isEqual zero)

    let mtx2 =
        createMatrixFromArray2D case.Format rightMatrix (isEqual zero)

    if mtx1.NNZ > 0 && mtx2.NNZ > 0 then
        try
            let m1 = mtx1.ToDevice case.TestContext.ClContext

            let m2 = mtx2.ToDevice case.TestContext.ClContext

            let res = addFun q HostInterop m1 m2

            m1.Dispose q
            m2.Dispose q

            let (cooRes: ClMatrix<'a>) = toCOOFun q HostInterop res
            let actual = cooRes.ToHost q

            cooRes.Dispose q
            res.Dispose q

            logger.debug (
                eventX "Actual is {actual}"
                >> setField "actual" (sprintf "%A" actual)
            )

            checkResult isEqual op zero leftMatrix rightMatrix actual
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let testFixturesEWiseAdd case =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolAdd =
          Matrix.map2 context ArithmeticOperations.boolSum wgSize

      let boolToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest false (||) boolAdd boolToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd =
          Matrix.map2 context ArithmeticOperations.intSum wgSize

      let intToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0 (+) intAdd intToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.map2 context ArithmeticOperations.floatSum wgSize

      let floatToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.map2 context ArithmeticOperations.byteSum wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let elementwiseAddTests =
    operationGPUTests "Backend.Matrix.EWiseAdd tests" testFixturesEWiseAdd

let testFixturesEWiseAddAtLeastOne case =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolAdd =
          Matrix.map2AtLeastOne context ArithmeticOperations.boolSumAtLeastOne wgSize

      let boolToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest false (||) boolAdd boolToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd =
          Matrix.map2AtLeastOne context ArithmeticOperations.intSumAtLeastOne wgSize

      let intToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0 (+) intAdd intToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.map2AtLeastOne context ArithmeticOperations.floatSumAtLeastOne wgSize

      let floatToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.map2AtLeastOne context ArithmeticOperations.byteSumAtLeastOne wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let elementwiseAddAtLeastOneTests =
    operationGPUTests "Backend.Matrix.EWiseAddAtLeastOne tests" testFixturesEWiseAddAtLeastOne

let testFixturesEWiseAddAtLeastOneToCOO case =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolAdd =
          Matrix.map2AtLeastOneToCOO context ArithmeticOperations.boolSumAtLeastOne wgSize

      let boolToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest false (||) boolAdd boolToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd =
          Matrix.map2AtLeastOneToCOO context ArithmeticOperations.intSumAtLeastOne wgSize

      let intToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0 (+) intAdd intToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.map2AtLeastOneToCOO context ArithmeticOperations.floatSumAtLeastOne wgSize

      let floatToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0.0 (+) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.map2AtLeastOneToCOO context ArithmeticOperations.byteSumAtLeastOne wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0uy (+) byteAdd byteToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let elementwiseAddAtLeastOneToCOOTests =
    operationGPUTests "Backend.Matrix.EWiseAddAtLeastOneToCOO tests" testFixturesEWiseAddAtLeastOneToCOO

let testFixturesEWiseMulAtLeastOne case =
    [ let config = defaultConfig
      let wgSize = 32

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case

      let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      let boolMul =
          Matrix.map2AtLeastOne context ArithmeticOperations.boolMulAtLeastOne wgSize

      let boolToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest false (&&) boolMul boolToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intAdd =
          Matrix.map2AtLeastOne context ArithmeticOperations.intMulAtLeastOne wgSize

      let intToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0 (*) intAdd intToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatAdd =
          Matrix.map2AtLeastOne context ArithmeticOperations.floatMulAtLeastOne wgSize

      let floatToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0.0 (*) floatAdd floatToCOO (fun x y -> abs (x - y) < Accuracy.medium.absolute) q
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let byteAdd =
          Matrix.map2AtLeastOne context ArithmeticOperations.byteMulAtLeastOne wgSize

      let byteToCOO = Matrix.toCOO context wgSize

      case
      |> correctnessGenericTest 0uy (*) byteAdd byteToCOO (=) q
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let elementwiseMulAtLeastOneTests =
    operationGPUTests "Backend.Matrix.eWiseMulAtLeastOne tests" testFixturesEWiseMulAtLeastOne
