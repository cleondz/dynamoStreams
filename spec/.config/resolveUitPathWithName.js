const resolveUitPath = require( 'resolve-uit-path' )();
const path = require( 'path' );

function resolveUitPathWithName( filePath ){

  const uitPath = resolveUitPath( filePath );
  const name = uitPath.replace( new RegExp( `((\\.\\.\\${ path.sep })+(lib\\${ path.sep })*)|\\.js`, 'g' ), '' );

  return Object.freeze( { uitPath, name } );

}

module.exports = resolveUitPathWithName;
