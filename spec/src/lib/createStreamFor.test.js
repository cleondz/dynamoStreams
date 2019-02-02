const uitPath           = resolveUitPath( __filename );
const testKey           = 'query';
const createStreamBoundToKey = require( uitPath )( testKey );

describe( 'Streams created by the createStreamFor function', ()=>{

  it( 'should invoke the method at the provided key of the supplied client instance with the supplied parameters', ()=>{

    const client = { query: sinon.spy() };
    const params = {};

    createStreamBoundToKey( client, params ).read();

    expect( client.query )
      .to.have.been.calledWith( params );

  } );

  it( 'should forward errors raised by the method', ()=>{

    const expected = new Error();
    const client   = { query: ( _, next )=>setImmediate( next, expected ) };

    createStreamBoundToKey( client, {} )
      .resume()
      .on( 'error', ( { error } )=>expect( error ).to.equal( expected ) );

  } );

  it( 'should include the parameters used to call the method when forwarding errors raised by it', ()=>{

    const expected = { foo: 'bar'};
    const client   = { query: ( _, next )=>setImmediate( next, new Error() ) };

    createStreamBoundToKey( client, expected )
      .resume()
      .on( 'error', ( { queryParameters } )=>expect( queryParameters ).to.equal( expected ) );

  } );

  it( 'should emit items returned by the method', done=>{

    const expected = [ { foo: 'foo' } ];
    const client   = { query: ( _, next )=>setImmediate( next, null, { Items: expected } ) };
    const actual   = [];

    createStreamBoundToKey( client, {} )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should end if no items are returned by the method', done=>{

    const client = { query: ( _, next )=>setImmediate( next, null, {} ) };

    createStreamBoundToKey( client, {} )
      .resume()
      .on( 'end',()=>{

        done();

      } );

  } );

  it( 'should end if an empty Items array is returned by the method', done=>{

    const client = { query: ( _, next )=>setImmediate( next, null, { Items: [] } ) };

    createStreamBoundToKey( client, {} )
      .resume()
      .on( 'end',()=>{

        done();

      } );

  } );

  it( 'should emit multiple items returned by the method', done=>{

    const expected = [ { foo: 'foo' }, { bar: 'bar' } ];
    const client   = { query: ( _, next )=>setImmediate( next, null, { Items: expected } ) };
    const actual   = [];

    createStreamBoundToKey( client, {} )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should skip nullable items returned by the method', done=>{

    const expected = [ { foo: 'foo' }, { bar: 'bar' } ];
    const client   = { query: ( _, next )=>setImmediate( next, null, { Items: [ null, undefined, ...expected ] } ) };
    const actual   = [];

    createStreamBoundToKey( client, {} )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );


  it( 'should emit all items, even if more items are returned than are read at one cycle', done=>{

    const expected = [ { foo: 'foo' }, { bar: 'bar' },  { baz: 'baz' }, { ban: 'ban' }   ];
    const client   = { query: ( _, next )=>setImmediate( next, null, { Items: expected } ) };
    const actual   = [];

    createStreamBoundToKey( client, {}, { highWaterMark: 1 } )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should, when a value for "LastEvaluatedKey" is returned, for more items', done=>{

    const expected = [ { foo: 'foo' }, { bar: 'bar' } ];

    const callResults = [
      { Items: [ expected[ 0 ] ], LastEvaluatedKey: 'someKey' },
      { Items: [ expected[ 1 ] ] }
    ];

    const client = { query: ( _, next )=>setImmediate( next, null, callResults.shift() ) };
    const actual = [];

    createStreamBoundToKey( client, {} )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should, when a value for "LastEvaluatedKey" is returned, first emit all items from the 1st call before calling the method again again', done=>{ // eslint-disable-line max-len

    const expected = [ { foo: 'foo' }, { bar: 'bar' },  { baz: 'baz' }, { ban: 'ban' } ];

    const callResults = [
      { Items: [ expected[ 0 ], expected[ 1 ], expected[ 2 ] ], LastEvaluatedKey: 'someKey' },
      { Items: [ expected[ 3 ] ] }
    ];

    const client = { query: ( _, next )=>setImmediate( next, null, callResults.shift() ) };
    const actual = [];

    createStreamBoundToKey( client, {}, { highWaterMark: 1 } )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should return just the items defined by Limit parameter even if exist LastEvaluatedKey', done=>{

    const query = { Limit : 2 };

    const expected = [ { foo: 'foo' }, { bar: 'bar' }];

    const callResults = [
      { Items: [ expected[ 0 ], expected[ 1 ] ], LastEvaluatedKey: 'someKey' },
      { Items: [ {} ] }
    ];

    const client = { query: ( _, next )=>setImmediate( next, null, callResults.shift() ) };
    const actual = [];

    createStreamBoundToKey( client, query, { highWaterMark: 1 } )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );


  it( 'should return just the items defined by Limit parameter even if first emit doesn\'t reach it ', done=>{ // eslint-disable-line max-len

    const query = { Limit : 4 };

    const expected = [ { foo: 'foo' }, { bar: 'bar' },  { baz: 'baz' }, { ban: 'ban' } ];

    const callResults = [
      { Items: [ expected[ 0 ], expected[ 1 ], expected[ 2 ] ], LastEvaluatedKey: 'someKey' },
      { Items: [ expected[ 3 ] ] , LastEvaluatedKey: 'anotherKey'},
      { Items: [ {} ] , LastEvaluatedKey: 'anotherKey'}

    ];

    const client = { query: ( _, next )=>setImmediate( next, null, callResults.shift() ) };
    const actual = [];

    createStreamBoundToKey( client, query, { highWaterMark: 1 } )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

} );
