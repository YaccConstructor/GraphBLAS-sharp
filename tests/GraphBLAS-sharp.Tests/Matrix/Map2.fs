module GraphBLAS.FSharp.Tests.Backend.Matrix.Map2

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.TestCases
open Microsoft.FSharp.Collections
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Map2.Tests"

let config = Utils.defaultConfig
let wgSize = Utils.defaultWorkGroupSize

let getCorrectnessTestName case datatype =
    $"Correctness on %s{datatype}, %A{case}"

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
        Utils.createMatrixFromArray2D case.Format leftMatrix (isEqual zero)

    let mtx2 =
        Utils.createMatrixFromArray2D case.Format rightMatrix (isEqual zero)

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

let creatTestMap2Add case (zero: 'a) add isEqual addQ map2 =
    let getCorrectnessTestName = getCorrectnessTestName case

    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    let map2 = map2 context addQ wgSize

    let toCOO = Matrix.toCOO context wgSize

    case
    |> correctnessGenericTest zero add map2 toCOO isEqual q
    |> testPropertyWithConfig config (getCorrectnessTestName $"{typeof<'a>}")

let testFixturesMap2Add case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      creatTestMap2Add case false (||) (=) ArithmeticOperations.boolSum Matrix.map2
      creatTestMap2Add case 0 (+) (=) ArithmeticOperations.intSum Matrix.map2

      if Utils.isFloat64Available context.ClDevice then
          creatTestMap2Add case 0.0 (+) Utils.floatIsEqual ArithmeticOperations.floatSum Matrix.map2

      creatTestMap2Add case 0.0f (+) Utils.float32IsEqual ArithmeticOperations.float32Sum Matrix.map2
      creatTestMap2Add case 0uy (+) (=) ArithmeticOperations.byteSum Matrix.map2 ]

let addTests =
    operationGPUTests "Backend.Matrix.map2 add tests" testFixturesMap2Add

let testFixturesMap2AddAtLeastOne case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      creatTestMap2Add case false (||) (=) ArithmeticOperations.boolSumAtLeastOne Matrix.map2AtLeastOne
      creatTestMap2Add case 0 (+) (=) ArithmeticOperations.intSumAtLeastOne Matrix.map2AtLeastOne

      if Utils.isFloat64Available context.ClDevice then
          creatTestMap2Add case 0.0 (+) Utils.floatIsEqual ArithmeticOperations.floatSumAtLeastOne Matrix.map2AtLeastOne

      creatTestMap2Add
          case
          0.0f
          (+)
          Utils.float32IsEqual
          ArithmeticOperations.float32SumAtLeastOne
          Matrix.map2AtLeastOne

      creatTestMap2Add case 0uy (+) (=) ArithmeticOperations.byteSumAtLeastOne Matrix.map2AtLeastOne ]


let addAtLeastOneTests =
    operationGPUTests "Backend.Matrix.map2AtLeastOne add tests" testFixturesMap2AddAtLeastOne

let testFixturesMap2AddAtLeastOneToCOO case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      creatTestMap2Add case false (||) (=) ArithmeticOperations.boolSumAtLeastOne Matrix.map2AtLeastOneToCOO
      creatTestMap2Add case 0 (+) (=) ArithmeticOperations.intSumAtLeastOne Matrix.map2AtLeastOneToCOO

      if Utils.isFloat64Available context.ClDevice then
          creatTestMap2Add
              case
              0.0
              (+)
              Utils.floatIsEqual
              ArithmeticOperations.floatSumAtLeastOne
              Matrix.map2AtLeastOneToCOO

      creatTestMap2Add
          case
          0.0f
          (+)
          Utils.float32IsEqual
          ArithmeticOperations.float32SumAtLeastOne
          Matrix.map2AtLeastOneToCOO

      creatTestMap2Add case 0uy (+) (=) ArithmeticOperations.byteSumAtLeastOne Matrix.map2AtLeastOneToCOO ]

let addAtLeastOneToCOOTests =
    operationGPUTests "Backend.Matrix.map2AtLeastOneToCOO add tests" testFixturesMap2AddAtLeastOneToCOO

let testFixturesMap2MulAtLeastOne case =
    [ let context = case.TestContext.ClContext
      let q = case.TestContext.Queue
      q.Error.Add(fun e -> failwithf "%A" e)

      creatTestMap2Add case false (&&) (=) ArithmeticOperations.boolMulAtLeastOne Matrix.map2AtLeastOne
      creatTestMap2Add case 0 (*) (=) ArithmeticOperations.intMulAtLeastOne Matrix.map2AtLeastOne

      if Utils.isFloat64Available context.ClDevice then
          creatTestMap2Add case 0.0 (*) Utils.floatIsEqual ArithmeticOperations.floatMulAtLeastOne Matrix.map2AtLeastOne

      creatTestMap2Add
          case
          0.0f
          (*)
          Utils.float32IsEqual
          ArithmeticOperations.float32MulAtLeastOne
          Matrix.map2AtLeastOne

      creatTestMap2Add case 0uy (*) (=) ArithmeticOperations.byteMulAtLeastOne Matrix.map2AtLeastOne ]

let mulAtLeastOneTests =
    operationGPUTests "Backend.Matrix.map2AtLeastOne multiplication tests" testFixturesMap2MulAtLeastOne
