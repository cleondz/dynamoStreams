function createReadStream( client, params, options ){

  let isReading = false;
  let hasRead   = false;
  let ExclusiveStartKey;

  const rest = [];

  function read( n ){

    if( isReading ) return;

    if( rest.length ){

      isReading = true;

      pushInitialItems.call( this, rest, n );
      isReading = false;

      this.push( rest.shift() );
      return;

    }

    if( hasRead ){

      if( ExclusiveStartKey == null ) return this.push( null );

      params            = { ...params, ExclusiveStartKey };
      ExclusiveStartKey = null;

    }

    hasRead   = true;
    isReading = true;

    client.query( params, ( err, result )=>{

      if( err ) return this.emit( 'error', err );
      if( !( result && result.Items && result.Items.length ) ) return this.push( null );

      ExclusiveStartKey = result.LastEvaluatedKey;

      const items  = [ ...result.Items ];

      pushInitialItems.call( this, items, n );
      isReading = false;

      const item = items.shift();

      rest.push( ...items );
      this.push( item );

    } );

  }

  return require( 'stream' ).Readable( { ...options, read, objectMode: true } );

}

module.exports = createReadStream;


function pushInitialItems( items, n ){

  while( items.length > 1 && n-- > 1 ) push.call( this, items.shift() );

}

function push( item ){

  if( item != null ) this.push( item );

}
